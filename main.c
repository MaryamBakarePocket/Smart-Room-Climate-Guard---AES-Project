/*
 * ESP32C3 Board | VS Code ESP-IDF Extension | FreeRTOS | Esp RainMaker (app, Insights, push alerts, voice, OTA)
 *
 *   Sensor   : DHT11 read when the 2 s timer notifies it
 *   Control  : LEDs, buzzer, fan LED, alarm logic 
 *   Cloud    : rate-limited RainMaker reporting
 *   Notify   : push alerts through RainMaker cloud
 *   Command  : the ONLY place that changes settings; app, voice and cloud writes all arrive here
 *   Conn     : Wi-Fi with cloud recovery supervisor
 *   OTA      : reacts to RainMaker OTA events
 *   Push Button   : short press = re-handshake, 3 s = Wi-Fi reset, 10 s = factory reset
 *   OLED     : local display
 */

#include <stdio.h>
#include <string.h>
#include <stdbool.h>
#include <math.h>

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/queue.h"
#include "freertos/semphr.h"
#include "freertos/timers.h"
#include "freertos/event_groups.h"

#include "driver/gpio.h"
#include "esp_rom_sys.h"
#include "esp_log.h"
#include "esp_event.h"
#include "esp_system.h"
#include "esp_wifi.h"
#include "esp_netif.h"
#include "esp_app_desc.h"
#include "nvs_flash.h"

#include <esp_rmaker_core.h>
#include <esp_rmaker_standard_types.h>
#include <esp_rmaker_standard_params.h>
#include <esp_rmaker_standard_devices.h>
#include <esp_rmaker_common_events.h>
#include <esp_rmaker_ota.h>
#include <esp_rmaker_utils.h>
#include <app_network.h>
#include <app_insights.h>

#include "dht.h"
#include "oled.h"

/* PINS (ESP32-C3) */
#define FAN_LED_PIN    GPIO_NUM_2      /* "Fan" (LED stands in for a fan relay) */
#define LED_GREEN_PIN  GPIO_NUM_3      /* Normal condition */
#define DHT_PIN        GPIO_NUM_4
#define BUZZER_PIN     GPIO_NUM_5      /* buzzer */
#define OLED_SDA       GPIO_NUM_6
#define OLED_SCL       GPIO_NUM_7
#define BUTTON_PIN     GPIO_NUM_9      /* BOOT button: short = re-handshake, 3 s = Wi-Fi reset, 10 s = factory reset */
#define LED_RED_PIN    GPIO_NUM_10     /* Alarm */

/* Buzzer */
#define BUZZER_PASSIVE   1      /* 1 = passive buzzer (tone), 0 = active buzzer (plain on/off) */
#define BUZZER_FREQ_HZ   2700   /* amplifies the buzzer*/

/*Event group bits */
#define CLOUD_UP_BIT   BIT0
#define WIFI_UP_BIT    BIT1


/* RainMaker allows about 12 messages per minute long-term; one report sends 4 params */
#define REPORT_MIN_GAP_MS    20000
#define REPORT_HEARTBEAT_MS  60000
#define REPORT_TEMP_STEP     1.0f
#define REPORT_HUM_STEP      3.0f

#define TEMP_HYST            1.0f      /* alarm clears only below limit - hysteresis */
#define HUM_HYST             3.0f
#define SENSOR_FAULT_COUNT   5         /* consecutive DHT failures before a fault alert */

#define WIFI_RETRY_MS        15000     /* Wi-Fi down: escalate after this */
#define CLOUD_RETRY_MS       60000     /* Wi-Fi up but cloud down: escalate after this */
#define REHANDSHAKE_RETRY_MS 30000
#define MAX_RECOVER_TRIES    6         /* then restart the chip */

#define MAX_TASKS            10

typedef struct {
    float temperature;
    float humidity;
    bool  valid;
} SensorData;

typedef struct {
    char msg[96];
} AlertMsg;

typedef enum {
    CMD_BUZZER_SET,     /* app / voice */
    CMD_FAN_SET,        /* app / voice (also switches Auto mode off) */
    CMD_AUTO_SET,       /* app */
    CMD_TEMP_SET,       /* app */
    CMD_HUM_SET,        /* app */
    CMD_FAN_STATE       /* internal: report fan state changed by Auto mode */
} cmd_type_t;

typedef struct {
    cmd_type_t type;
    float f;
    bool  b;
} Cmd;

typedef enum {
    CONN_WIFI_LOST,
    CONN_WIFI_UP,
    CONN_CLOUD_LOST,
    CONN_LINK_OK,
    CONN_RECOVER,       /* watchdog fired and we are still down */
    CONN_REHANDSHAKE    /* manual button */
} conn_evt_t;

static const char *TAG = "CLIMATE_GUARD";

static QueueHandle_t ctrl_q, cloud_q, oled_q, alert_q, btn_q, cmd_q, conn_q, ota_q;
static SemaphoreHandle_t data_mutex;
static EventGroupHandle_t net_events;
static TaskHandle_t sensor_task_handle;
static TaskHandle_t task_list[MAX_TASKS];
static int task_count = 0;
static TimerHandle_t sample_timer, blink_timer, recover_timer, stack_timer;

static SensorData last_data;
static volatile float temp_limit = 28.0f;
static volatile float hum_limit = 75.0f;
static volatile bool manual_buzzer = false;
static volatile bool alarm_muted = false;
static volatile bool alarm_active = false;
static volatile bool manual_fan = false;
static volatile bool auto_mode = true;
static volatile bool fan_state = false;
static volatile bool ota_busy = false;
static volatile TickType_t press_tick = 0;

static esp_rmaker_device_t *sensor_dev, *buzzer_dev, *fan_dev;
static esp_rmaker_param_t *temp_param, *hum_param, *dew_param, *status_param;
static esp_rmaker_param_t *temp_limit_param, *hum_limit_param;
static esp_rmaker_param_t *buzzer_power_param, *fan_power_param, *auto_param;

/* Dew point estimation*/
static float dew_point(float t, float rh)
{
    if (rh < 1.0f) {
        rh = 1.0f;
    }
    const float a = 17.62f, b = 243.12f;
    float g = logf(rh / 100.0f) + (a * t) / (b + t);
    return (b * g) / (a - g);
}

static void queue_alert(const char *text)
{
    AlertMsg a;
    snprintf(a.msg, sizeof(a.msg), "%s", text);
    xQueueSend(alert_q, &a, 0);
}

/* Re-run the control logic with the latest reading (after a setting changes) */
static void poke_control(void)
{
    SensorData d;
    xSemaphoreTake(data_mutex, portMAX_DELAY);
    d = last_data;
    xSemaphoreGive(data_mutex);
    xQueueOverwrite(ctrl_q, &d);
}

static void spawn(TaskFunction_t fn, const char *name, uint32_t stack, UBaseType_t prio,
                  TaskHandle_t *out)
{
    TaskHandle_t h = NULL;
    if (xTaskCreate(fn, name, stack, NULL, prio, &h) != pdPASS) {
        ESP_LOGE(TAG, "Could not create task %s, restarting", name);
        esp_restart();
    }
    if (out) {
        *out = h;
    }
    if (task_count < MAX_TASKS) {
        task_list[task_count++] = h;
    }
}

static bool wifi_has_credentials(void)
{
    wifi_config_t cfg;
    return esp_wifi_get_config(WIFI_IF_STA, &cfg) == ESP_OK && cfg.sta.ssid[0] != 0;
}

/* ---------------- Buzzer driver ----------------
 * Passive buzzer: a small task makes a BUZZER_FREQ_HZ tone (loudest for most piezos).
 * Active buzzer: plain on/off (set BUZZER_PASSIVE to 0). */
#if BUZZER_PASSIVE
static volatile bool buzzer_want = false;
static TaskHandle_t buzzer_task_handle = NULL;

static void buzzer_task(void *arg)
{
    const uint32_t half_us = 1000000UL / (2UL * BUZZER_FREQ_HZ);   /* 185 us at 2700 Hz */
    const int cycles = 9600 / (int)(2 * half_us);                  /* ~9.6 ms burst */

    for (;;) {
        if (!buzzer_want) {
            gpio_set_level(BUZZER_PIN, 0);
            ulTaskNotifyTake(pdTRUE, portMAX_DELAY);               /* sleep until asked to sound */
            continue;
        }
        for (int i = 0; i < cycles && buzzer_want; i++) {
            gpio_set_level(BUZZER_PIN, 1);
            esp_rom_delay_us(half_us);
            gpio_set_level(BUZZER_PIN, 0);
            esp_rom_delay_us(half_us);
        }
        vTaskDelay(1);                                             /* let other tasks and the watchdog run */
    }
}
#endif

static void buzzer_set(bool on)
{
#if BUZZER_PASSIVE
    buzzer_want = on;
    if (on && buzzer_task_handle) {
        xTaskNotifyGive(buzzer_task_handle);
    }
#else
    gpio_set_level(BUZZER_PIN, on ? 1 : 0);
#endif
}

/* ---------------- RainMaker write callback ----------------
 * App and voice (Alexa / Google) commands both arrive here. It only enqueues;
 * the Command task does the work. */
static esp_err_t write_cb(const esp_rmaker_device_t *device, const esp_rmaker_param_t *param,
                          const esp_rmaker_param_val_t val, void *priv_data,
                          esp_rmaker_write_ctx_t *ctx)
{
    Cmd c = {0};
    const char *pname = esp_rmaker_param_get_name(param);

    if (device == buzzer_dev && strcmp(pname, ESP_RMAKER_DEF_POWER_NAME) == 0) {
        c.type = CMD_BUZZER_SET;
        c.b = val.val.b;
    } else if (device == fan_dev && strcmp(pname, ESP_RMAKER_DEF_POWER_NAME) == 0) {
        c.type = CMD_FAN_SET;
        c.b = val.val.b;
    } else if (param == auto_param) {
        c.type = CMD_AUTO_SET;
        c.b = val.val.b;
    } else if (param == temp_limit_param) {
        c.type = CMD_TEMP_SET;
        c.f = val.val.f;
    } else if (param == hum_limit_param) {
        c.type = CMD_HUM_SET;
        c.f = val.val.f;
    } else {
        return ESP_OK;
    }

    if (xQueueSend(cmd_q, &c, 0) != pdTRUE) {
        ESP_LOGW(TAG, "Command queue full, command dropped");
        return ESP_FAIL;
    }
    return ESP_OK;
}

/* Event handlers */
static void event_handler(void *arg, esp_event_base_t base, int32_t id, void *data)
{
    conn_evt_t e;

    if (base == WIFI_EVENT && id == WIFI_EVENT_STA_DISCONNECTED) {
        xEventGroupClearBits(net_events, WIFI_UP_BIT | CLOUD_UP_BIT);
        e = CONN_WIFI_LOST;
        xQueueSend(conn_q, &e, 0);
    } else if (base == IP_EVENT && id == IP_EVENT_STA_GOT_IP) {
        xEventGroupSetBits(net_events, WIFI_UP_BIT);
        e = CONN_WIFI_UP;
        xQueueSend(conn_q, &e, 0);
    } else if (base == RMAKER_COMMON_EVENT) {
        if (id == RMAKER_MQTT_EVENT_CONNECTED) {
            xEventGroupSetBits(net_events, CLOUD_UP_BIT);
            e = CONN_LINK_OK;
            xQueueSend(conn_q, &e, 0);
        } else if (id == RMAKER_MQTT_EVENT_DISCONNECTED) {
            xEventGroupClearBits(net_events, CLOUD_UP_BIT);
            e = CONN_CLOUD_LOST;
            xQueueSend(conn_q, &e, 0);
        }
    }
}

static void ota_event_handler(void *arg, esp_event_base_t base, int32_t id, void *data)
{
    xQueueSend(ota_q, &id, 0);
}

/*  Timers */
static void sample_timer_cb(TimerHandle_t t)
{
    xTaskNotifyGive(sensor_task_handle);
}

static void blink_timer_cb(TimerHandle_t t)
{
    static bool on = false;
    on = alarm_active ? !on : false;
    gpio_set_level(LED_RED_PIN, on ? 1 : 0);
}

static void recover_timer_cb(TimerHandle_t t)
{
    conn_evt_t e = CONN_RECOVER;
    xQueueSend(conn_q, &e, 0);
}

/* One-shot: log stack headroom of every task (put the table in the report) */
static void stack_timer_cb(TimerHandle_t t)
{
    for (int i = 0; i < task_count; i++) {
        ESP_LOGI(TAG, "Stack high-water mark %-8s: %u bytes free",
                 pcTaskGetName(task_list[i]),
                 (unsigned)uxTaskGetStackHighWaterMark(task_list[i]));
    }
    ESP_LOGI(TAG, "Free heap: %u bytes", (unsigned)esp_get_free_heap_size());
}

/*  Reset / re-handshake button  */
static void IRAM_ATTR button_isr(void *arg)
{
    BaseType_t woken = pdFALSE;
    if (gpio_get_level(BUTTON_PIN) == 0) {
        press_tick = xTaskGetTickCountFromISR();
    } else {
        TickType_t held = xTaskGetTickCountFromISR() - press_tick;
        xQueueSendFromISR(btn_q, &held, &woken);
    }
    if (woken) {
        portYIELD_FROM_ISR();
    }
}

static void button_task(void *arg)
{
    TickType_t held;
    for (;;) {
        if (xQueueReceive(btn_q, &held, portMAX_DELAY) != pdTRUE) {
            continue;
        }
        uint32_t ms = pdTICKS_TO_MS(held);
        if (ms < 50) {
            continue;                                   /* contact bounce */
        }
        if (ms >= 10000) {
            ESP_LOGW(TAG, "Factory reset");
            esp_rmaker_factory_reset(2, 2);
        } else if (ms >= 3000) {
            ESP_LOGW(TAG, "Wi-Fi reset: re-provision with the RainMaker app");
            esp_rmaker_wifi_reset(2, 2);
        } else {
            ESP_LOGW(TAG, "Short press: manual re-handshake");
            conn_evt_t e = CONN_REHANDSHAKE;
            xQueueSend(conn_q, &e, 0);
        }
    }
}

/*  Connectivity supervisor  */
static void rehandshake(void)
{
    if (!wifi_has_credentials()) {
        ESP_LOGW(TAG, "No Wi-Fi credentials yet: provision with the RainMaker app (hold button 3 s to reset)");
        return;
    }
    ESP_LOGW(TAG, "Re-handshake: reconnecting Wi-Fi and cloud");
    esp_wifi_disconnect();
    esp_err_t err = esp_wifi_connect();
    if (err != ESP_OK) {
        ESP_LOGW(TAG, "esp_wifi_connect: %s", esp_err_to_name(err));
    }
    xTimerChangePeriod(recover_timer, pdMS_TO_TICKS(REHANDSHAKE_RETRY_MS), 0);
}

static void conn_task(void *arg)
{
    conn_evt_t e;
    int attempts = 0;

    for (;;) {
        if (xQueueReceive(conn_q, &e, portMAX_DELAY) != pdTRUE) {
            continue;
        }
        switch (e) {
        case CONN_WIFI_LOST:
            ESP_LOGW(TAG, "Wi-Fi lost");
            xTimerChangePeriod(recover_timer, pdMS_TO_TICKS(WIFI_RETRY_MS), 0);
            break;
        case CONN_WIFI_UP:
            ESP_LOGI(TAG, "Wi-Fi connected");
            if (!(xEventGroupGetBits(net_events) & CLOUD_UP_BIT)) {
                xTimerChangePeriod(recover_timer, pdMS_TO_TICKS(CLOUD_RETRY_MS), 0);
            }
            break;
        case CONN_CLOUD_LOST:
            ESP_LOGW(TAG, "Cloud connection lost");
            xTimerChangePeriod(recover_timer, pdMS_TO_TICKS(CLOUD_RETRY_MS), 0);
            break;
        case CONN_LINK_OK:
            ESP_LOGI(TAG, "Cloud connected");
            xTimerStop(recover_timer, 0);
            attempts = 0;
            break;
        case CONN_RECOVER:
            if (!wifi_has_credentials()) {
                break;                                  /* still provisioning, do not interfere */
            }
            if (++attempts >= MAX_RECOVER_TRIES) {
                ESP_LOGE(TAG, "Recovery failed %d times, restarting", attempts);
                esp_restart();
            }
            rehandshake();
            break;
        case CONN_REHANDSHAKE:
            attempts = 0;
            rehandshake();
            break;
        }
    }
}

/*  TASK: sensor reading  */
static void sensor_task(void *arg)
{
    int fails = 0;

    for (;;) {
        ulTaskNotifyTake(pdTRUE, portMAX_DELAY);        /* woken by the 2 s timer */

        SensorData d = {0};
        float h = 0, t = 0;
        esp_err_t err = dht_read_float_data(DHT_TYPE_DHT11, DHT_PIN, &h, &t);
        if (err == ESP_OK) {
            d.temperature = t;
            d.humidity = h;
            d.valid = true;
            fails = 0;
            ESP_LOGI(TAG, "Temp: %.1f C  Humidity: %.1f %%", (double)t, (double)h);
        } else {
            ESP_LOGW(TAG, "DHT read failed: %s", esp_err_to_name(err));
            if (++fails == SENSOR_FAULT_COUNT) {
                queue_alert("Sensor fault: check DHT wiring");
            }
        }

        xSemaphoreTake(data_mutex, portMAX_DELAY);
        last_data = d;
        xSemaphoreGive(data_mutex);

        xQueueOverwrite(ctrl_q, &d);
        xQueueOverwrite(cloud_q, &d);
        xQueueOverwrite(oled_q, &d);
    }
}

/*  TASK: LEDs, buzzer, fan, alarm logic  */
static void control_task(void *arg)
{
    bool was_alarm = false;

    for (;;) {
        SensorData d;
        if (xQueueReceive(ctrl_q, &d, portMAX_DELAY) != pdTRUE) {
            continue;
        }

        bool alarm = was_alarm;                         /* keep last state on a failed read */

        if (d.valid) {
            if (was_alarm) {
                alarm = d.temperature >= (temp_limit - TEMP_HYST)
                     || d.humidity >= (hum_limit - HUM_HYST);
            } else {
                alarm = d.temperature >= temp_limit || d.humidity >= hum_limit;
            }

            gpio_set_level(LED_GREEN_PIN, alarm ? 0 : 1);

            if (alarm && !was_alarm) {
                alarm_muted = false;                    /* a new alarm starts audible */
                alarm_active = true;
                xTimerStart(blink_timer, 0);

                AlertMsg a;
                if (d.temperature >= temp_limit) {
                    snprintf(a.msg, sizeof(a.msg),
                             "Warning! High Temperature Detected: %.0f\xC2\xB0" "C",
                             (double)d.temperature);
                } else {
                    snprintf(a.msg, sizeof(a.msg),
                             "Warning! High Humidity Detected: %.0f %%", (double)d.humidity);
                }
                xQueueSend(alert_q, &a, 0);
            } else if (!alarm && was_alarm) {
                alarm_muted = false;
                alarm_active = false;
                xTimerStop(blink_timer, 0);
                gpio_set_level(LED_RED_PIN, 0);
            }
            was_alarm = alarm;
        }

        /* Buzzer: alarm (unless muted) or manual switch */
        buzzer_set((alarm && !alarm_muted) || manual_buzzer);

        /* Fan: Auto follows the alarm, Manual follows the switch */
        bool fan_out = auto_mode ? alarm : manual_fan;
        if (fan_out != fan_state) {
            fan_state = fan_out;
            gpio_set_level(FAN_LED_PIN, fan_out ? 1 : 0);
            if (auto_mode) {                            /* tell the app, via the Command task */
                Cmd r = { .type = CMD_FAN_STATE, .b = fan_out };
                xQueueSend(cmd_q, &r, 0);
            }
        }
    }
}

/*  TASK: commands from app, voice and the control task  */
static void command_task(void *arg)
{
    Cmd c;

    for (;;) {
        if (xQueueReceive(cmd_q, &c, portMAX_DELAY) != pdTRUE) {
            continue;
        }

        switch (c.type) {
        case CMD_BUZZER_SET:
            manual_buzzer = c.b;
            alarm_muted = !c.b;                         /* "buzzer off" also silences an alarm */
            ESP_LOGI(TAG, "Buzzer: %s", c.b ? "ON" : "OFF");
            esp_rmaker_param_update_and_report(buzzer_power_param, esp_rmaker_bool(c.b));
            break;
        case CMD_FAN_SET:
            manual_fan = c.b;
            auto_mode = false;                          /* manual command takes over */
            ESP_LOGI(TAG, "Fan: %s (manual)", c.b ? "ON" : "OFF");
            esp_rmaker_param_update_and_report(fan_power_param, esp_rmaker_bool(c.b));
            esp_rmaker_param_update_and_report(auto_param, esp_rmaker_bool(false));
            break;
        case CMD_AUTO_SET:
            auto_mode = c.b;
            ESP_LOGI(TAG, "Auto mode: %s", c.b ? "ON" : "OFF");
            esp_rmaker_param_update_and_report(auto_param, esp_rmaker_bool(c.b));
            break;
        case CMD_TEMP_SET:
            temp_limit = c.f;
            ESP_LOGI(TAG, "Temperature limit: %.0f", (double)c.f);
            esp_rmaker_param_update_and_report(temp_limit_param, esp_rmaker_float(c.f));
            break;
        case CMD_HUM_SET:
            hum_limit = c.f;
            ESP_LOGI(TAG, "Humidity limit: %.0f", (double)c.f);
            esp_rmaker_param_update_and_report(hum_limit_param, esp_rmaker_float(c.f));
            break;
        case CMD_FAN_STATE:
            esp_rmaker_param_update_and_report(fan_power_param, esp_rmaker_bool(c.b));
            continue;                                   /* no need to re-run control */
        }
        poke_control();
    }
}

/*  TASK: cloud communication (rate limited)  */
static void cloud_task(void *arg)
{
    float last_t = 0, last_h = 0;
    bool last_alarm = false;
    bool first = true;
    TickType_t last_report = 0;

    for (;;) {
        SensorData d;
        if (xQueueReceive(cloud_q, &d, portMAX_DELAY) != pdTRUE || !d.valid) {
            continue;
        }

        if (!(xEventGroupGetBits(net_events) & CLOUD_UP_BIT)) {
            first = true;                               /* send a full report once the cloud is back */
            continue;
        }

        bool alarm = d.temperature >= temp_limit || d.humidity >= hum_limit;
        TickType_t now = xTaskGetTickCount();
        uint32_t since_ms = pdTICKS_TO_MS(now - last_report);

        if (!first && since_ms < REPORT_MIN_GAP_MS && alarm == last_alarm) {
            continue;                                   /* too soon, protect the MQTT budget */
        }

        bool changed = first
                    || alarm != last_alarm
                    || fabsf(d.temperature - last_t) >= REPORT_TEMP_STEP
                    || fabsf(d.humidity - last_h) >= REPORT_HUM_STEP;
        bool heartbeat = since_ms >= REPORT_HEARTBEAT_MS;

        if (!changed && !heartbeat) {
            continue;
        }

        esp_rmaker_param_update_and_report(temp_param, esp_rmaker_float(d.temperature));
        esp_rmaker_param_update_and_report(hum_param, esp_rmaker_float(d.humidity));
        esp_rmaker_param_update_and_report(dew_param,
                                           esp_rmaker_float(dew_point(d.temperature, d.humidity)));
        esp_rmaker_param_update_and_report(status_param,
                                           esp_rmaker_str(alarm ? "ALARM" : "Normal"));

        last_t = d.temperature;
        last_h = d.humidity;
        last_alarm = alarm;
        last_report = now;
        first = false;
    }
}

/*  TASK: push notifications  */
static void notify_task(void *arg)
{
    for (;;) {
        AlertMsg a;
        if (xQueueReceive(alert_q, &a, portMAX_DELAY) == pdTRUE) {
            /* wait (event driven) until the cloud is reachable */
            xEventGroupWaitBits(net_events, CLOUD_UP_BIT, pdFALSE, pdTRUE, portMAX_DELAY);
            esp_err_t err = esp_rmaker_raise_alert(a.msg);
            ESP_LOGW(TAG, "Alert \"%s\" -> %s", a.msg, esp_err_to_name(err));
        }
    }
}

/* ---------------- TASK: OTA events ---------------- */
static void ota_task(void *arg)
{
    int32_t id;

    for (;;) {
        if (xQueueReceive(ota_q, &id, portMAX_DELAY) != pdTRUE) {
            continue;
        }
        switch (id) {
        case RMAKER_OTA_EVENT_STARTING:
        case RMAKER_OTA_EVENT_IN_PROGRESS:
            ota_busy = true;
            ESP_LOGI(TAG, "OTA update in progress");
            break;
        case RMAKER_OTA_EVENT_SUCCESSFUL:
            ESP_LOGI(TAG, "OTA update installed, device will reboot");
            queue_alert("Firmware update installed, rebooting");
            break;
        case RMAKER_OTA_EVENT_FAILED:
        case RMAKER_OTA_EVENT_REJECTED:
            ota_busy = false;
            ESP_LOGW(TAG, "OTA failed or rejected (event %ld)", (long)id);
            break;
        default:
            break;
        }
    }
}

/*  TASK: OLED  */
static void oled_task(void *arg)
{
    char line[64];

    for (;;) {
        SensorData d;
        if (xQueueReceive(oled_q, &d, portMAX_DELAY) != pdTRUE) {
            continue;
        }
        oled_clear();
        if (d.valid) {
            snprintf(line, sizeof(line), "T:%.1fC H:%.0f%%",
                     (double)d.temperature, (double)d.humidity);
            oled_text(0, line);
            snprintf(line, sizeof(line), "Dew:%.1f Fan:%s",
                     (double)dew_point(d.temperature, d.humidity), fan_state ? "ON" : "OFF");
            oled_text(1, line);
        } else {
            oled_text(0, "Sensor error!");
            oled_text(1, "Check DHT wiring");
        }
        snprintf(line, sizeof(line), "Lim T:%.0f H:%.0f", (double)temp_limit, (double)hum_limit);
        oled_text(2, line);

        EventBits_t bits = xEventGroupGetBits(net_events);
        if (ota_busy) {
            oled_text(3, "OTA updating...");
        } else {
            snprintf(line, sizeof(line), "WiFi:%s Cld:%s",
                     (bits & WIFI_UP_BIT) ? "OK" : "--",
                     (bits & CLOUD_UP_BIT) ? "OK" : "--");
            oled_text(3, line);
        }
        oled_show();
    }
}

/* Helpers to build RainMaker params */
static esp_rmaker_param_t *make_limit_param(const char *name, float def, float min, float max)
{
    esp_rmaker_param_t *p = esp_rmaker_param_create(name, "custom.param.limit",
                                                    esp_rmaker_float(def),
                                                    PROP_FLAG_READ | PROP_FLAG_WRITE);
    esp_rmaker_param_add_ui_type(p, ESP_RMAKER_UI_SLIDER);
    esp_rmaker_param_add_bounds(p, esp_rmaker_float(min), esp_rmaker_float(max),
                                esp_rmaker_float(1));
    return p;
}

static esp_rmaker_param_t *make_text_param(const char *name, const char *type,
                                           esp_rmaker_param_val_t val)
{
    esp_rmaker_param_t *p = esp_rmaker_param_create(name, type, val, PROP_FLAG_READ);
    esp_rmaker_param_add_ui_type(p, ESP_RMAKER_UI_TEXT);
    return p;
}

/* MAIN */
void app_main(void)
{
    /* NVS */
    esp_err_t err = nvs_flash_init();
    if (err == ESP_ERR_NVS_NO_FREE_PAGES || err == ESP_ERR_NVS_NEW_VERSION_FOUND) {
        ESP_ERROR_CHECK(nvs_flash_erase());
        err = nvs_flash_init();
    }
    ESP_ERROR_CHECK(err);

    ESP_LOGI(TAG, "Smart Climate Guard, firmware version %s", esp_app_get_description()->version);

    /* RTOS objects */
    ctrl_q  = xQueueCreate(1, sizeof(SensorData));
    cloud_q = xQueueCreate(1, sizeof(SensorData));
    oled_q  = xQueueCreate(1, sizeof(SensorData));
    alert_q = xQueueCreate(4, sizeof(AlertMsg));
    btn_q   = xQueueCreate(4, sizeof(TickType_t));
    cmd_q   = xQueueCreate(8, sizeof(Cmd));
    conn_q  = xQueueCreate(8, sizeof(conn_evt_t));
    ota_q   = xQueueCreate(8, sizeof(int32_t));
    data_mutex = xSemaphoreCreateMutex();
    net_events = xEventGroupCreate();
    if (!ctrl_q || !cloud_q || !oled_q || !alert_q || !btn_q || !cmd_q || !conn_q || !ota_q
        || !data_mutex || !net_events) {
        ESP_LOGE(TAG, "RTOS object creation failed, restarting");
        esp_restart();
    }

    /* GPIO: buzzer + LEDs */
    gpio_config_t out_cfg = {
        .pin_bit_mask = (1ULL << BUZZER_PIN) | (1ULL << LED_GREEN_PIN)
                      | (1ULL << LED_RED_PIN) | (1ULL << FAN_LED_PIN),
        .mode = GPIO_MODE_OUTPUT,
        .pull_up_en = GPIO_PULLUP_DISABLE,
        .pull_down_en = GPIO_PULLDOWN_DISABLE,
        .intr_type = GPIO_INTR_DISABLE,
    };
    ESP_ERROR_CHECK(gpio_config(&out_cfg));
    gpio_set_level(BUZZER_PIN, 0);
    gpio_set_level(LED_GREEN_PIN, 0);
    gpio_set_level(LED_RED_PIN, 0);
    gpio_set_level(FAN_LED_PIN, 0);

    /* GPIO: reset / re-handshake button (interrupt on both edges) */
    gpio_config_t btn_cfg = {
        .pin_bit_mask = (1ULL << BUTTON_PIN),
        .mode = GPIO_MODE_INPUT,
        .pull_up_en = GPIO_PULLUP_ENABLE,
        .pull_down_en = GPIO_PULLDOWN_DISABLE,
        .intr_type = GPIO_INTR_ANYEDGE,
    };
    ESP_ERROR_CHECK(gpio_config(&btn_cfg));
    err = gpio_install_isr_service(0);
    if (err != ESP_OK && err != ESP_ERR_INVALID_STATE) {
        ESP_ERROR_CHECK(err);
    }
    ESP_ERROR_CHECK(gpio_isr_handler_add(BUTTON_PIN, button_isr, NULL));

    /* OLED (the rest of the system still runs if it is missing) */
    if (oled_init(OLED_SDA, OLED_SCL) != ESP_OK) {
        ESP_LOGW(TAG, "OLED not found at 0x3C, continuing without it");
    }

    /* Wi-Fi / network layer, then our event handlers */
    app_network_init();
    ESP_ERROR_CHECK(esp_event_handler_register(WIFI_EVENT, WIFI_EVENT_STA_DISCONNECTED,
                                               &event_handler, NULL));
    ESP_ERROR_CHECK(esp_event_handler_register(IP_EVENT, IP_EVENT_STA_GOT_IP,
                                               &event_handler, NULL));
    ESP_ERROR_CHECK(esp_event_handler_register(RMAKER_COMMON_EVENT, ESP_EVENT_ANY_ID,
                                               &event_handler, NULL));
    ESP_ERROR_CHECK(esp_event_handler_register(RMAKER_OTA_EVENT, ESP_EVENT_ANY_ID,
                                               &ota_event_handler, NULL));

    /* RainMaker node */
    esp_rmaker_config_t rainmaker_cfg = {
        .enable_time_sync = true,                       /* real timestamps for alerts and Insights */
    };
    esp_rmaker_node_t *node = esp_rmaker_node_init(&rainmaker_cfg, "Climate Guard Node",
                                                   "Climate Guard");
    if (!node) {
        ESP_LOGE(TAG, "Could not initialise RainMaker node, restarting");
        esp_restart();
    }

    /* Device 1: temperature sensor (Alexa / Google can read it) */
    sensor_dev = esp_rmaker_temp_sensor_device_create("Room Sensor", NULL, 0.0f);
    esp_rmaker_device_add_cb(sensor_dev, write_cb, NULL);
    temp_param = esp_rmaker_device_get_param_by_type(sensor_dev, ESP_RMAKER_PARAM_TEMPERATURE);

    hum_param = make_text_param("Humidity", "custom.param.humidity", esp_rmaker_float(0));
    esp_rmaker_device_add_param(sensor_dev, hum_param);
    dew_param = make_text_param("Dew Point", "custom.param.dewpoint", esp_rmaker_float(0));
    esp_rmaker_device_add_param(sensor_dev, dew_param);
    status_param = make_text_param("Status", "custom.param.status", esp_rmaker_str("Normal"));
    esp_rmaker_device_add_param(sensor_dev, status_param);

    temp_limit_param = make_limit_param("Temp Limit", 28.0f, 10.0f, 60.0f);
    esp_rmaker_device_add_param(sensor_dev, temp_limit_param);
    hum_limit_param = make_limit_param("Humidity Limit", 75.0f, 20.0f, 95.0f);
    esp_rmaker_device_add_param(sensor_dev, hum_limit_param);
    esp_rmaker_node_add_device(node, sensor_dev);

    /* Device 2: buzzer switch (voice: "turn on / off the buzzer") */
    buzzer_dev = esp_rmaker_switch_device_create("Buzzer", NULL, false);
    esp_rmaker_device_add_cb(buzzer_dev, write_cb, NULL);
    buzzer_power_param = esp_rmaker_device_get_param_by_type(buzzer_dev, ESP_RMAKER_PARAM_POWER);
    esp_rmaker_node_add_device(node, buzzer_dev);

    /* Device 3: fan switch + Auto Mode (voice: "turn on the fan") */
    fan_dev = esp_rmaker_switch_device_create("Fan", NULL, false);
    esp_rmaker_device_add_cb(fan_dev, write_cb, NULL);
    fan_power_param = esp_rmaker_device_get_param_by_type(fan_dev, ESP_RMAKER_PARAM_POWER);
    auto_param = esp_rmaker_param_create("Auto Mode", "custom.param.auto",
                                         esp_rmaker_bool(true),
                                         PROP_FLAG_READ | PROP_FLAG_WRITE);
    esp_rmaker_param_add_ui_type(auto_param, ESP_RMAKER_UI_TOGGLE);
    esp_rmaker_device_add_param(fan_dev, auto_param);
    esp_rmaker_node_add_device(node, fan_dev);

    /* OTA + Insights */
    esp_rmaker_ota_enable_default();
    err = app_insights_enable();
    if (err != ESP_OK) {
        ESP_LOGW(TAG, "Insights not enabled: %s", esp_err_to_name(err));
    }

    /* Tasks and timers start BEFORE the network, so local control works offline */
    spawn(control_task, "Control", 3072, 4, NULL);
    spawn(command_task, "Command", 4096, 4, NULL);
    spawn(conn_task,    "Conn",    3072, 4, NULL);
    spawn(sensor_task,  "Sensor",  4096, 3, &sensor_task_handle);
    spawn(button_task,  "Button",  3072, 3, NULL);
    spawn(cloud_task,   "Cloud",   4096, 2, NULL);
    spawn(notify_task,  "Notify",  4096, 2, NULL);
    spawn(ota_task,     "OTA",     3072, 2, NULL);
    spawn(oled_task,    "OLED",    4096, 1, NULL);
#if BUZZER_PASSIVE
    spawn(buzzer_task,  "Buzzer",  2048, 1, &buzzer_task_handle);
#endif

    sample_timer  = xTimerCreate("sample",  pdMS_TO_TICKS(2000),  pdTRUE,  NULL, sample_timer_cb);
    blink_timer   = xTimerCreate("blink",   pdMS_TO_TICKS(300),   pdTRUE,  NULL, blink_timer_cb);
    recover_timer = xTimerCreate("recover", pdMS_TO_TICKS(WIFI_RETRY_MS), pdFALSE, NULL,
                                 recover_timer_cb);
    stack_timer   = xTimerCreate("stacks",  pdMS_TO_TICKS(60000), pdFALSE, NULL, stack_timer_cb);
    if (!sample_timer || !blink_timer || !recover_timer || !stack_timer) {
        ESP_LOGE(TAG, "Timer creation failed, restarting");
        esp_restart();
    }
    xTimerStart(sample_timer, 0);
    xTimerStart(stack_timer, 0);

    /* Start RainMaker and Wi-Fi provisioning / connection */
    esp_rmaker_start();

    err = app_network_start(POP_TYPE_RANDOM);
    if (err != ESP_OK) {
        ESP_LOGE(TAG, "Could not start Wi-Fi: %s, restarting", esp_err_to_name(err));
        esp_restart();
    }
}
