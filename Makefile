# -----------------------------
# Compiler & flags
# -----------------------------
CC := clang
AR := ar
ARFLAGS := rcs
CFLAGS := -Wall -Wextra -Werror -pedantic -std=c11 -g -fsanitize=address -MMD -MP -Iinclude -Isrc -D_GNU_SOURCE -D_XOPEN_SOURCE=700

# -----------------------------
# System libraries (pkg-config)
# -----------------------------
NGHTTP2_CFLAGS := $(shell pkg-config --cflags libnghttp2 2>/dev/null)
NGHTTP2_LIBS := $(shell pkg-config --libs libnghttp2 2>/dev/null)
URING_CFLAGS := $(shell pkg-config --cflags liburing 2>/dev/null)
URING_LIBS := $(shell pkg-config --libs liburing 2>/dev/null)
SSL_CFLAGS := $(shell pkg-config --cflags openssl 2>/dev/null)
SSL_LIBS := $(shell pkg-config --libs openssl 2>/dev/null)
COMPRESS_CFLAGS := $(shell pkg-config --cflags zlib libzstd libbrotlienc libbrotlidec 2>/dev/null)
COMPRESS_LIBS := $(shell pkg-config --libs zlib libzstd libbrotlienc libbrotlidec 2>/dev/null)
CFLAGS += $(NGHTTP2_CFLAGS) $(URING_CFLAGS) $(SSL_CFLAGS) $(COMPRESS_CFLAGS)

# -----------------------------
# Directories
# -----------------------------
SRC_DIR := src
BUILD_DIR := build
BIN_DIR := bin
LIB_DIR := lib
TARGET := $(BIN_DIR)/program

# -----------------------------
# Modules
# -----------------------------
# Library modules: directories starting with lib*
LIB_SRC_DIRS := $(shell find $(SRC_DIR) -mindepth 1 -maxdepth 1 -type d -name 'lib*')
LIB_NAMES := $(notdir $(LIB_SRC_DIRS))

# App modules: directories not starting with lib*
APP_SRC_DIRS := $(shell find $(SRC_DIR) -mindepth 1 -maxdepth 1 -type d ! -name 'lib*')
APP_NAMES := $(notdir $(APP_SRC_DIRS))

# -----------------------------
# Unity source files (one per module)
# -----------------------------
LIB_ENTRY_SRCS := $(foreach lib,$(LIB_NAMES),$(SRC_DIR)/$(lib)/$(lib).c)
APP_ENTRY_SRCS := $(foreach app,$(APP_NAMES),$(SRC_DIR)/$(app)/$(app).c)

# Check that entry files exist
$(foreach f,$(LIB_ENTRY_SRCS) $(APP_ENTRY_SRCS),\
  $(if $(wildcard $(f)),,$(error Missing required module entry file: $(f)))\
)

# -----------------------------
# Object files
# -----------------------------
LIB_OBJS := $(patsubst $(SRC_DIR)/%.c,$(BUILD_DIR)/%.o,$(LIB_ENTRY_SRCS))
APP_OBJS := $(patsubst $(SRC_DIR)/%.c,$(BUILD_DIR)/%.o,$(APP_ENTRY_SRCS))

# -----------------------------
# Library files (Internal + External)
# -----------------------------
INTERNAL_LIBS := $(patsubst %,$(LIB_DIR)/%.a,$(LIB_NAMES))

EXTERNAL_NAMES := $(shell awk '!/^[[:space:]]*#/ && NF { \
  sub(/^override[[:space:]]+/, ""); \
  gsub(/@.*$/, ""); \
  n = split($$1, parts, "/"); \
  print parts[n]; \
}' excalibur.txt 2>/dev/null)
EXTERNAL_LIBS := $(patsubst %,$(LIB_DIR)/%.a,$(EXTERNAL_NAMES))

STATIC_LIB_FILES := $(INTERNAL_LIBS) $(EXTERNAL_LIBS)

# -----------------------------
# Dependencies
# -----------------------------
DEPS := $(LIB_OBJS:.o=.d) $(APP_OBJS:.o=.d)

# -----------------------------
# Compile rule for unity files
# -----------------------------
$(BUILD_DIR)/%.o: $(SRC_DIR)/%.c
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -c $< -o $@

# -----------------------------
# Build static library for each internal lib module
# -----------------------------
define MAKE_STATIC_LIB
$(LIB_DIR)/$1.a: $(BUILD_DIR)/$1/$1.o
	@mkdir -p $(LIB_DIR)
	$(AR) $(ARFLAGS) $$@ $$^
endef

$(foreach lib,$(LIB_NAMES),$(eval $(call MAKE_STATIC_LIB,$(lib))))

# -----------------------------
# Dependency Resolution
# -----------------------------
# If excalibur.txt is newer than our stamp file, or the stamp is missing, run install
$(LIB_DIR)/.excal.stamp: excalibur.txt
	@echo "Checking dependencies..."
	excalibur install
	@mkdir -p $(LIB_DIR)
	@touch $(LIB_DIR)/.excal.stamp

# -----------------------------
# Main program
# -----------------------------
MAIN_OBJ := $(BUILD_DIR)/main.o

# -----------------------------
# Final executable
# -----------------------------
# Ensure the final executable waits for dependency resolution FIRST
$(TARGET): $(LIB_DIR)/.excal.stamp $(MAIN_OBJ) $(APP_OBJS) $(STATIC_LIB_FILES)
	@mkdir -p $(BIN_DIR)
	$(CC) $(CFLAGS) $(MAIN_OBJ) $(APP_OBJS) $(STATIC_LIB_FILES) $$(find $(LIB_DIR) -maxdepth 1 -name '*.a' 2>/dev/null) $(NGHTTP2_LIBS) $(URING_LIBS) $(SSL_LIBS) $(COMPRESS_LIBS) -o $@

all: $(TARGET)

run: $(TARGET)
	$(TARGET)

clean:
	rm -rf $(BUILD_DIR) $(BIN_DIR)
	rm -f $(INTERNAL_LIBS)
	rm -f $(LIB_DIR)/.excal.stamp

-include $(DEPS)

.PHONY: all clean run
