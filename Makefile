# Rocky 9:  make
# CentOS 7: scl enable devtoolset-7 -- make

CXX      ?= g++
CXXFLAGS ?= -std=c++17 -O2 -Wall -Wextra -Iinclude
LDFLAGS  ?=
LDLIBS   ?= -lpthread

SRC := $(wildcard src/*.cpp)
OBJ := $(patsubst src/%.cpp, build/%.o, $(SRC))
BIN := market_data

.PHONY: all clean

all: $(BIN)

$(BIN): $(OBJ)
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $^ $(LDLIBS)

build/%.o: src/%.cpp
	@mkdir -p build
	$(CXX) $(CXXFLAGS) -c -o $@ $<

clean:
	rm -rf build $(BIN)

