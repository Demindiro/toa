## Format

Pages are put in "<name>.pages.compr"
Table is put in "<name>.table.compr"

### Table header

| bytes | name                  |
| -----:|:--------------------- |
|   7:0 | magic "Compress"      |
|  11:8 | version 0x20260317    |
| 15:12 | page size             |
| 16:16 | compression algorithm |
| 17:17 | compression level     |
| 31:18 | (zero)                |

### Table entry

| bytes | name               |
| -----:|:------------------ |
|   7:0 | offset             |
|   8:8 | algorithm          |
|  11:9 | compressed length  |
| 15:12 | (zero)             |
