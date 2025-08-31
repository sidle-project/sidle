#include <cstdint>
#include <string>

#include "helper.h"

#if !defined(MICRO_BASE_H)
#define MICRO_BASE_H
/**********************************************************************
 * Key & value
 *********************************************************************/

#if defined(VAL_8)
#define VAL_LEN 0
#elif defined(VAL_16)
#define VAL_LEN 1
#elif defined(VAL_24)
#define VAL_LEN 2
#elif defined(VAL_40)
#define VAL_LEN 4
#elif defined(VAL_32)
#define VAL_LEN 3
#elif defined(VAL_64)
#define VAL_LEN 7
#elif defined(VAL_128)
#define VAL_LEN 15
#elif defined(VAL_256)
#define VAL_LEN 31
#elif defined(VAL_512)
#define VAL_LEN 63
#elif defined(VAL_1024)
#define VAL_LEN 127
#elif defined(VAL_2048)
#define VAL_LEN 255
#elif defined(VAL_4096)
#define VAL_LEN 511
#else
#define VAL_LEN 0
#endif

template <size_t len, class feat, size_t model_unit = 8>
class StrBagKey {
 public:
  static constexpr size_t model_key_size() {
    // return (size_t)std::ceil(1.0 * len / model_unit);
    return (len % model_unit) == 0 ? (len / model_unit)
                                   : (len / model_unit + 1);
  }
  typedef std::array<feat, model_key_size()> model_key_t;

  static StrBagKey max() {
    static StrBagKey max_key;
    memset(&max_key.buf, 255, len);
    return max_key;
  }
  static StrBagKey min() {
    static StrBagKey min_key;
    memset(&min_key.buf, 0, len);
    return min_key;
  }

  StrBagKey() { memset(&buf, 0, len); }
  StrBagKey(uint64_t key) { COUT_N_EXIT("fuck! str key no uint64"); }
  StrBagKey(const std::string &s) {
    assert(s.size() <= len);
    memset(&buf, 0, len);
    memcpy(&buf, s.data(), s.size());
  }
  StrBagKey(const StrBagKey &other) { memcpy(&buf, &other.buf, len); }
  StrBagKey &operator=(const StrBagKey &other) {
    memcpy(&buf, &other.buf, len);
    return *this;
  }

  model_key_t to_model_key() const {
    model_key_t model_key;
    for (size_t k_i = 0; k_i < model_key_size() - 1; k_i++) {
      uint8_t temp[model_unit];
      for (size_t byte_i = 0; byte_i < model_unit; ++byte_i) {
        if (little_endian()) {
          temp[model_unit - 1 - byte_i] = buf[k_i * model_unit + byte_i];
        } else {
          temp[byte_i] = buf[k_i * model_unit + byte_i];
        }
      }
      model_key[k_i] = *(uint64_t *)&temp;
    }

    // handle trailing bytes
    size_t trailing_byte_n = len - (model_key_size() - 1) * model_unit;
    uint8_t temp[trailing_byte_n];
    for (size_t byte_i = 0; byte_i < trailing_byte_n; ++byte_i) {
      if (little_endian()) {
        temp[trailing_byte_n - 1 - byte_i] =
            buf[(model_key_size() - 1) * model_unit + byte_i];
      } else {
        temp[byte_i] = buf[(model_key_size() - 1) * model_unit + byte_i];
      }
    }
    if (trailing_byte_n == 1) {
      model_key[model_key_size() - 1] = *(uint8_t *)&temp;
    } else if (trailing_byte_n == 2) {
      model_key[model_key_size() - 1] = *(uint16_t *)&temp;
    } else if (trailing_byte_n == 4) {
      model_key[model_key_size() - 1] = *(uint32_t *)&temp;
    } else if (trailing_byte_n == 8) {
      model_key[model_key_size() - 1] = *(uint64_t *)&temp;
    } else {
      COUT_N_EXIT("the len of the key is not ok!! len=" << len);
    }

    return model_key;
  }

  StrBagKey to_str_key() const { return *this; }
  StrBagKey to_wh_key() const { return *this; }
  StrBagKey to_normal_key() const { return to_str_key(); }

  friend bool operator<(const StrBagKey &l, const StrBagKey &r) {
    return memcmp(&l.buf, &r.buf, len) < 0;
  }
  friend bool operator>(const StrBagKey &l, const StrBagKey &r) {
    return memcmp(&l.buf, &r.buf, len) > 0;
  }
  friend bool operator>=(const StrBagKey &l, const StrBagKey &r) {
    return memcmp(&l.buf, &r.buf, len) >= 0;
  }
  friend bool operator<=(const StrBagKey &l, const StrBagKey &r) {
    return memcmp(&l.buf, &r.buf, len) <= 0;
  }
  friend bool operator==(const StrBagKey &l, const StrBagKey &r) {
    return memcmp(&l.buf, &r.buf, len) == 0;
  }
  friend bool operator!=(const StrBagKey &l, const StrBagKey &r) {
    return memcmp(&l.buf, &r.buf, len) != 0;
  }

  int keycmp(const model_key_t lk, const model_key_t rk, size_t l) {
    for (size_t i = 0; i < l; ++i) {
      if (lk[i] > rk[i]) return 1;
      if (lk[i] < rk[i]) return -1;
    }
    return 0;
  }

  friend std::ostream &operator<<(std::ostream &os, const StrBagKey &key) {
    os << "key [" << std::hex;
    for (size_t i = 0; i < sizeof(StrBagKey); i++) {
      os << "0x" << key.buf[i] << " ";
    }
    os << "] (as byte)" << std::dec;
    os << ", model_key:[";
    model_key_t m_k = key.to_model_key();
    for (size_t k_i = 0; k_i < model_key_size(); k_i++) {
      os << m_k[k_i] << " ";
    }
    os << "].";
    return os;
  }

  uint8_t buf[len];
} PACKED;

template <size_t len, class feat>
class StrKey {
  typedef std::array<feat, len> model_key_t;

 public:
  static constexpr size_t model_key_size() { return len; }

  static StrKey max() {
    static StrKey max_key;
    memset(&max_key.buf, 255, len);
    return max_key;
  }
  static StrKey min() {
    static StrKey min_key;
    memset(&min_key.buf, 0, len);
    return min_key;
  }

  StrKey() { memset(&buf, 0, len); }
  StrKey(uint64_t key) { COUT_N_EXIT("fuck! str key no uint64"); }
  StrKey(const std::string &s) {
    memset(&buf, 0, len);
    memcpy(&buf, s.data(), s.size());
  }
  StrKey(const StrKey &other) { memcpy(&buf, &other.buf, len); }
  StrKey &operator=(const StrKey &other) {
    memcpy(&buf, &other.buf, len);
    return *this;
  }

  model_key_t to_model_key() const {
    model_key_t model_key;
    for (size_t i = 0; i < len; i++) {
      model_key[i] = buf[i];
    }
    return model_key;
  }

  StrKey to_str_key() const { return *this; }
  StrKey to_wh_key() const { return *this; }
  StrKey to_normal_key() const { return to_str_key(); }

  friend bool operator<(const StrKey &l, const StrKey &r) {
    return memcmp(&l.buf, &r.buf, len) < 0;
  }
  friend bool operator>(const StrKey &l, const StrKey &r) {
    return memcmp(&l.buf, &r.buf, len) > 0;
  }
  friend bool operator>=(const StrKey &l, const StrKey &r) {
    return memcmp(&l.buf, &r.buf, len) >= 0;
  }
  friend bool operator<=(const StrKey &l, const StrKey &r) {
    return memcmp(&l.buf, &r.buf, len) <= 0;
  }
  friend bool operator==(const StrKey &l, const StrKey &r) {
    return memcmp(&l.buf, &r.buf, len) == 0;
  }
  friend bool operator!=(const StrKey &l, const StrKey &r) {
    return memcmp(&l.buf, &r.buf, len) != 0;
  }

  friend std::ostream &operator<<(std::ostream &os, const StrKey &key) {
    os << "key [" << std::hex;
    for (size_t i = 0; i < sizeof(StrKey); i++) {
      os << "0x" << key.buf[i] << " ";
    }
    os << "] (as byte)" << std::dec;
    return os;
  }

  uint8_t buf[len];
} PACKED;

template <class feat_t>
class Key {
  typedef std::array<feat_t, 1> model_key_t;

 public:
  static constexpr size_t model_key_size() { return 1; }
  static Key max() {
    static Key max_key(std::numeric_limits<uint64_t>::max());
    return max_key;
  }
  static Key min() {
    static Key min_key(std::numeric_limits<uint64_t>::min());
    return min_key;
  }

  Key() : key(0) {}
  Key(uint64_t key) : key(key) {}
  Key(const Key &other) { key = other.key; }
  Key &operator=(const Key &other) {
    key = other.key;
    return *this;
  }

  model_key_t to_model_key() const {
    model_key_t model_key;
    model_key[0] = key;
    return model_key;
  }

  Key to_str_key() const {
    if (little_endian()) {
      Key reversed;
      for (size_t byte_i = 0; byte_i < sizeof(uint64_t); byte_i++) {
        ((char *)&reversed)[byte_i] =
            ((char *)this)[sizeof(uint64_t) - byte_i - 1];
      }
      return reversed;
    }
    return *this;
  }

  Key to_wh_key() const { return to_str_key(); }
  Key to_normal_key() const { return to_str_key(); }

  friend bool operator<(const Key &l, const Key &r) { return l.key < r.key; }
  friend bool operator>(const Key &l, const Key &r) { return l.key > r.key; }
  friend bool operator>=(const Key &l, const Key &r) { return l.key >= r.key; }
  friend bool operator<=(const Key &l, const Key &r) { return l.key <= r.key; }
  friend bool operator==(const Key &l, const Key &r) { return l.key == r.key; }
  friend bool operator!=(const Key &l, const Key &r) { return l.key != r.key; }

  friend std::ostream &operator<<(std::ostream &os, const Key &key) {
    os << "key [" << std::hex;
    for (size_t i = 0; i < sizeof(uint64_t); i++) {
      os << "0x" << (int)((unsigned char *)&key.key)[i] << " ";
    }
    os << "] (as byte), [0x" << key.key << "] (as uint64_t)" << std::dec;
    return os;
  }

  uint64_t key;
} PACKED;

class Value {
 public:
#if VAL_LEN > 0
  std::array<int64_t, VAL_LEN> padding;
#endif
  int64_t val;
  // friend bool operator<(const Value &l, const Value &r) {
  //   return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(Value)) < 0;
  // }
  // friend bool operator>(const Value &l, const Value &r) {
  //   return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(Value)) > 0;
  // }
  friend bool operator==(const Value &l, const Value &r) {
    return l.val == r.val;
  }
  friend bool operator!=(const Value &l, const Value &r) {
    return l.val != r.val;
  }
} PACKED;

/**********************************************************************
 * random function
 *********************************************************************/

// NOTE: not thread-safe
// taken from java:
//   http://developer.classpath.org/doc/java/util/Random-source.html
class FastRandom {
 public:
  FastRandom(unsigned long seed) : seed(0) { set_seed0(seed); }

  inline unsigned long next() {
    return ((unsigned long)next(32) << 32) + next(32);
  }

  inline uint32_t next_u32() { return next(32); }

  inline uint16_t next_u16() { return next(16); }

  /** [0.0, 1.0) */
  inline double next_uniform() {
    return ((next(26) << 27) + next(27)) / (double)(1L << 53);
  }

  inline char next_char() { return next(8) % 256; }

  inline char next_readable_char() {
    static const char readables[] =
        "0123456789@ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz";
    return readables[next(6)];
  }

  inline std::string next_string(size_t len) {
    std::string s(len, 0);
    for (size_t i = 0; i < len; i++) s[i] = next_char();
    return s;
  }

  inline std::string next_readable_string(size_t len) {
    std::string s(len, 0);
    for (size_t i = 0; i < len; i++) s[i] = next_readable_char();
    return s;
  }

  inline unsigned long get_seed() { return seed; }

  inline void set_seed(unsigned long seed) { this->seed = seed; }

 private:
  inline void set_seed0(unsigned long seed) {
    this->seed = (seed ^ 0x5DEECE66DL) & ((1L << 48) - 1);
  }

  inline unsigned long next(unsigned int bits) {
    seed = (seed * 0x5DEECE66DL + 0xBL) & ((1L << 48) - 1);
    return seed >> (48 - bits);
  }

  unsigned long seed;
};

// NOTE: this function is very vulnerable to integer overflow
static inline ALWAYS_INLINE int64_t rand_int64(FastRandom &r, int64_t min,
                                               int64_t max) {
  int64_t res = (int64_t)(r.next_uniform() * (max - min + 1) + min);
  assert(res >= min && res <= max);
  return res;
}

#endif  // MICRO_BASE_H