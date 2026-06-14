## Protocol

u24 | user data
u8  | command

Packets are at most 4 + 8192 bytes large.


### 1. Status

Request: n/a

response:

u256 | root
...  | reserved


### 2. Fetch

Request:

u256 | hash

Response:

ty = 
u1   | chunk or pair
u1   | data or refs
u1   | not found or valid
u5   | (zero)

data =
[u8] | data

### 3. Store data chunk

Request:

[u8] | data

### 4. Store data pair

Request:

[u256; 2] | data

### 5. Store refs chunk

Request:

[u256] | data

### 6. Store refs pair

Request:

[u256; 2] | data
