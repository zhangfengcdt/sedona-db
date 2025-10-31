#pragma once
#define ENCODE_DE(A, B)                                                                \
  (                                      /* 1. Mask A (24 bits) and shift left by 8 */ \
   (((uint32_t)(A) & 0xFFFFFFUL) << 8) | /* 2. Mask B (8 bits) and leave in place */   \
   (((uint32_t)(B) & 0xFFUL)))

#define ENCODE_UINT32_T_3(A, B, C)                                                     \
  (                                     /* 1. Mask A (16 bits) and shift left by 16 */ \
   (((uint32_t)(A) & 0xFFFFUL) << 16) | /* 2. Mask B (8 bits) and shift left by 8 */   \
   (((uint32_t)(B) & 0xFFUL) << 8) |    /* 3. Mask C (8 bits) and leave in place */    \
   (((uint32_t)(C) & 0xFFUL)))
