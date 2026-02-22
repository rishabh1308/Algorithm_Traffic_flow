
#include "model/ConflictMatrix.hpp"

namespace tip::model {

    ConflictMatrix::ConflictMatrix(const std::vector<Lane>& lanes)
        : n_(lanes.size())
        , mask_(n_, LaneMask{0})
    {
        if (n_ > 64) {
            throw std::runtime_error(
                "ConflictMatrix: lane count (" + std::to_string(n_) +
                ") exceeds maximum of 64");
        }

        for (std::size_t i = 0; i < n_; ++i) {
            for (std::size_t j = i + 1; j < n_; ++j) {
                if (polylinesIntersect(lanes[i].path, lanes[j].path)) {
                    mask_[i] |= (LaneMask{1} << j);
                    mask_[j] |= (LaneMask{1} << i);
                }
            }
        }
    }

    bool ConflictMatrix::isFeasible(LaneMask activeMask) const noexcept {
        LaneMask remaining = activeMask;
        while (remaining) {
            // Extract lowest set bit index
            int i = __builtin_ctzll(remaining);
            // Check if this lane conflicts with any other active lane
            // (exclude self by clearing own bit)
            LaneMask others = activeMask & ~(LaneMask{1} << i);
            if (mask_[static_cast<std::size_t>(i)] & others) {
                return false;
            }
            remaining &= remaining - 1; // clear lowest set bit
        }
        return true;
    }

}