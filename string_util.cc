#include "string_util.h"
#include <assert.h>

void split_string(const std::string& str,
                  char sep,
                  std::vector<std::string>* stringlist) {
  assert(stringlist != NULL);
  size_t last = 0;
  for (size_t i = 0; i <= str.size(); i++) {
    if (i == str.size() || str[i] == sep) {
      stringlist->push_back(str.substr(last, i - last));
      last = i+1;
    }
  }
}

Slice TrimSpace(Slice s) {
  size_t start = 0;
  while (start < s.size() && isspace(s[start])) {
    start++;
  }
  size_t limit = s.size();
  while (limit > start && isspace(s[limit-1])) {
    limit--;
  }
  return Slice(s.data() + start, limit - start);
}

