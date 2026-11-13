#pragma once
#include <stdbool.h>
#include "esp_err.h"
#include "driver/gpio.h"

#ifdef __cplusplus
extern "C" {
#endif

/* SSD1306 128x32 OLED over I2C, address 0x3C.
 * Returns ESP_OK if the screen answered. If it did not, every other
 * function below simply does nothing, so the rest of the app keeps running. */
esp_err_t oled_init(gpio_num_t sda, gpio_num_t scl);

/* Wipe the drawing buffer (the screen only changes after oled_show) */
void oled_clear(void);

/* Write text on row 0..3 (each row is 8 pixels high, 21 characters wide) */
void oled_text(int row, const char *str);

/* Send the drawing buffer to the screen */
void oled_show(void);

#ifdef __cplusplus
}
#endif
