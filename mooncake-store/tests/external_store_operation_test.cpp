#include "external_store_operation.h"

#include <iostream>
#include <stdexcept>

using namespace mooncake;
using namespace mooncake::external_detail;

static void Require(bool ok) {
    if (!ok) throw std::runtime_error("external result assertion failed");
}

int main() {
    auto error =
        tl::expected<void, ErrorCode>{tl::unexpected(ErrorCode::WRITE_FAIL)};
    // Per-key combinations: both succeed, native only, external only, neither.
    auto result = MergePutResults(Results{{}, {}, error, error},
                                  Results{{}, error, {}, error});
    Require(result.size() == 4 && result[0] && result[1] && result[2] &&
            !result[3] && result[3].error() == ErrorCode::WRITE_FAIL);

    Results malformed{{}};
    CheckResults(malformed, 4);
    result = MergePutResults(Results(4, error), malformed);
    Require(result.size() == 4);
    for (const auto& entry : result) Require(!entry);

    Results rejected(4, tl::unexpected(ErrorCode::TRANSFER_FAIL));
    CheckResults(rejected, 4);
    result = MergePutResults(Results{{}, error, {}, error}, rejected);
    Require(result[0] && !result[1] && result[2] && !result[3]);
    Require(bool(MergePutResults(Results{error}, Results{{}})[0]));
    Require(MergePutResults({}, {}).empty());

    // Invalid native shape cannot manufacture a native success.
    result = MergePutResults(Results{{}}, Results{{}, error});
    Require(result.size() == 2 && result[0] && !result[1]);

    int data = 0;
    Require(ValidWrite({"a"}, {{{&data, sizeof(data)}}}));
    Require(!ValidWrite({"a", "a"},
                        {{{&data, sizeof(data)}}, {{&data, sizeof(data)}}}));
    Require(!ValidWrite({"a"}, {{{nullptr, sizeof(data)}}}));
    std::cout << "external result tests passed\n";
}
