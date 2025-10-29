#pragma once

#include <cstdint>
#include "gpuspatial/config.h"


// --- Standard-Compliant Type Punning Helper ---
// This union allows us to treat the same 32 bits of memory as either an unsigned integer
// or a single-precision floating-point number (float), which is the safest way
// to perform bit-level reinterpretation in C and C++ before C++20's std::bit_cast.
union FloatBitConverter {
    uint32_t i; // 32-bit unsigned integer view
    float f;    // 32-bit float view
};

// --- Bit Configuration (New) ---
// PIP_RT_POLYGON occupies the 22 most-significant bits (MSB).
// PIP_RT_RING occupies the 10 least-significant bits (LSB).
#define POLYGON_BITS 22 // PIP_RT_POLYGON (MSB)
#define RING_BITS 10 // PIP_RT_RING (LSB)

// Maximum mask for POLYGON (22 bits: 0x3FFFFF)
#define POLYGON_MASK ((1U << POLYGON_BITS) - 1)
// Maximum mask for RING (10 bits: 0x3FF)
#define RING_MASK ((1U << RING_BITS) - 1)
// Shift amount for POLYGON (equal to RING_BITS)
#define POLYGON_SHIFT RING_BITS // 10

/**
 * ENCODE_PIP_RT Macro
 * Encodes two integers (POLYGON and RING) into the bit pattern of a 32-bit float.
 *
 * @param POLYGON The first integer (22 MSBs, max value 4,194,303).
 * @param RING The second integer (10 LSBs, max value 1023).
 * @return A 32-bit float whose bit pattern is the encoded data.
 */
#define ENCODE_PIP_RT(POLYGON, RING) ({ \
    /* Use a temporary union variable to perform the type-punning safely */ \
    FloatBitConverter converter; \
    /* 1. Mask POLYGON to 22 bits, then shift POLYGON left by 10 bits (POLYGON_SHIFT) */ \
    uint32_t polygon_shifted = (((uint32_t)(POLYGON) & POLYGON_MASK) << POLYGON_SHIFT); \
    /* 2. Mask RING to 10 bits */ \
    uint32_t ring_masked = ((uint32_t)(RING) & RING_MASK); \
    /* 3. Combine the bit patterns */ \
    converter.i = polygon_shifted | ring_masked; \
    /* 4. Access the combined bit pattern as a float */ \
    converter.f; \
})

/**
 * DECODE_PIP_RT_POLYGON Macro
 * Decodes the 22 MSB integer (POLYGON) from the bit pattern of the packed float.
 *
 * @param PACKED_FLOAT The encoded 32-bit float.
 * @return The first integer (POLYGON).
 */
#define DECODE_PIP_RT_POLYGON(PACKED_FLOAT) ({ \
    /* Use a temporary union variable to perform the type-punning safely */ \
    FloatBitConverter converter; \
    converter.f = (PACKED_FLOAT); \
    /* 1. Extract the underlying integer bit pattern */ \
    uint32_t combined_int = converter.i; \
    /* 2. Shift right by 10 (POLYGON_SHIFT) to move the 22 MSBs into the LSB position */ \
    /* 3. Mask with POLYGON_MASK (0x3FFFFF) to isolate the 22 bits */ \
    (combined_int >> POLYGON_SHIFT) & POLYGON_MASK; \
})

/**
 * DECODE_PIP_RT_RING Macro
 * Decodes the 10 LSB integer (RING) from the bit pattern of the packed float.
 *
 * @param PACKED_FLOAT The encoded 32-bit float.
 * @return The second integer (RING).
 */
#define DECODE_PIP_RT_RING(PACKED_FLOAT) ({ \
    /* Use a temporary union variable to perform the type-punning safely */ \
    FloatBitConverter converter; \
    converter.f = (PACKED_FLOAT); \
    /* 1. Extract the underlying integer bit pattern */ \
    uint32_t combined_int = converter.i; \
    /* 2. Mask the integer with RING_MASK (0x3FF) to isolate the 10 LSBs */ \
    combined_int & RING_MASK; \
})

