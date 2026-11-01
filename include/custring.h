#ifndef CUSTRING_H
#define CUSTRING_H

#include <stdbool.h>
#include <stddef.h>

/* 1. OPAQUE BASE TYPE */
typedef struct FixedString_s FixedString;

/* 2. THE GENERATOR MACRO */
#define DEFINE_FIXED_STRING(N)                                                 \
  typedef struct {                                                             \
    size_t length;                                                             \
    size_t capacity;                                                           \
    char data[N];                                                              \
  } FixedString##N

/* 3. STANDARD SIZE DEFINITIONS */
DEFINE_FIXED_STRING(5);
DEFINE_FIXED_STRING(8);
DEFINE_FIXED_STRING(16);
DEFINE_FIXED_STRING(32);
DEFINE_FIXED_STRING(64);
DEFINE_FIXED_STRING(128);
DEFINE_FIXED_STRING(256);

/* 4. INTERNAL HELPER */
#define FSTR(x) ((FixedString *)(x))

/*
 * 5. INITIALIZATION MACROS
 * Automatically calculates the capacity from the type's `data` array
 * at compile time using a null-pointer sizeof evaluation.
 */
#define INIT_FSTR(type)                                                        \
  (type) { .length = 0, .capacity = sizeof(((type *)0)->data), .data = {0} }

#define INIT_FSTR_LIT(type, str)                                               \
  (type) {                                                                     \
    .length = sizeof("" str) - 1, .capacity = sizeof(((type *)0)->data),       \
    .data = str                                                                \
  }

/* --- The Underlying C Implementations --- */
void fstr_assign_impl(FixedString *fstr, const char *src);
void fstr_append_impl(FixedString *fstr, const char *src);
void fstr_append_char_impl(FixedString *fstr, char c);
void fstr_clear_impl(FixedString *fstr);

/*
 * =========================================================================
 * USER API MACROS
 * These auto-cast your specific struct pointers to the opaque base pointer.
 * =========================================================================
 */
#define fstr_assign(fstr_ptr, src) fstr_assign_impl(FSTR(fstr_ptr), (src))
#define fstr_append(fstr_ptr, src) fstr_append_impl(FSTR(fstr_ptr), (src))
#define fstr_append_char(fstr_ptr, c) fstr_append_char_impl(FSTR(fstr_ptr), (c))
#define fstr_clear(fstr_ptr) fstr_clear_impl(FSTR(fstr_ptr))

#endif // CUSTRING_H
