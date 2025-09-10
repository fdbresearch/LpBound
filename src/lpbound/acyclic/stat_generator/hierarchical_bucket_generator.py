import math


def get_quantiles(values: list[int | float | str], num_bins: int) -> list[int | float | str]:
    """
    Computes 'num_bins' approximate quantile boundaries for a sorted list of values.
    Returns a list of boundary values (right edges).
    """
    if len(values) == 0 or num_bins <= 0:
        return []

    sorted_vals: list[int | float | str] = sorted(values)
    edges: list[int | float | str] = []
    n: int = len(sorted_vals)
    for i in range(1, num_bins + 1):
        idx: int = int(n * (i / (num_bins + 1)))
        idx = min(idx, n - 1)
        edges.append(sorted_vals[idx])
    return edges


class HierarchicalBucketGenerator:
    """
    Generates a hierarchical (layered) histogram that includes:
      - Base intervals at each 2^k layer (base_{step_size})
      - Offset intervals for step sizes >= 4 (offset_{step_size})

    Each "base_*" or "offset_*" set of intervals is treated as a separate layer.

    Buckets are stored as tuples:
        (lower_bound, upper_bound, step_size, label)

    For example:
      (None, 1901, 1, "base_1") => The leftmost interval of base_1 is open-ended on the left side.
    """

    def __init__(self, values: list[int | float | str | None], num_bins: int):
        """
        :param values: The list of column values to bucketize.
        :param num_bins: The number of desired bins (at the finest granularity).
        """
        # Remove None
        cleaned_values: list[int | float | str] = [v for v in values if v is not None]

        # Sort values
        self.values_sorted: list[int | float | str] = sorted(cleaned_values)
        self.count: int = len(self.values_sorted)
        if self.count == 0:
            # No data => no intervals
            self.num_bins: int = 0
            self.edges: list[int | float | str] = []
            self.step_sizes: list[int] = []
            return

        self.min_value: int | float | str = self.values_sorted[0]
        self.max_value: int | float | str = self.values_sorted[-1]

        # Clamp num_bins not to exceed data size
        self.num_bins = min(num_bins, self.count)
        # Round up num_bins to next power of two
        if self.num_bins > 0:
            self.num_bins = 2 ** math.ceil(math.log2(self.num_bins))

        # Compute approximate quantiles
        self.edges = get_quantiles(self.values_sorted, self.num_bins) if self.num_bins > 0 else []

        # Prepare each layer's step size: [2^0, 2^1, 2^2, ..., 2^k]
        if self.num_bins > 0:
            num_levels = int(math.log2(self.num_bins)) + 1
            self.step_sizes = [2**k for k in range(num_levels)]
        else:
            self.step_sizes = []

    def _finalize_open_ended(
        self, intervals: list[tuple[int | float | None, int | float | None, int, str]]
    ) -> list[tuple[int | float | None, int | float | None, int, str]]:
        """
        Given a list of intervals for a single layer (either base_* or offset_*),
        make them open-ended on the leftmost and rightmost intervals.

        If there is exactly one interval, make it (None, None).
        Otherwise, make the leftmost (None, x) and the rightmost (y, None).
        The rest remain as-is.

        Returns a new list of intervals with open-ended outer boundaries.
        """
        if not intervals:
            return intervals

        # Sort intervals by ascending lower bound (lexicographically for strings)
        if isinstance(intervals[0][0], str):
            intervals.sort(key=lambda itv: ("" if itv[0] is None else str(itv[0])))
        else:
            intervals.sort(key=lambda itv: (itv[0]))

        if len(intervals) == 1:
            # Only 1 interval => [None, None]
            (_, _, sz, lbl) = intervals[0]
            intervals[0] = (None, None, sz, lbl)
            return intervals

        # Multiple intervals => leftmost gets None for lower bound, rightmost gets None for upper bound
        (_, r0, sz0, lbl0) = intervals[0]
        intervals[0] = (None, r0, sz0, lbl0)

        (lN, _, szN, lblN) = intervals[-1]
        intervals[-1] = (lN, None, szN, lblN)

        return intervals

    def generate_buckets(self) -> list[tuple[int | float | None, int | float | None, int, str]]:
        """
        Build the list of intervals. For each step_size, we consider:
          1) base_{step_size} intervals => treat as one layer
          2) offset_{step_size} intervals (if step_size >= 4) => treat as another layer

        We finalize each "layer" by making it open-ended on both ends (or [None, None] if single interval).
        Then we append those to a final list. We do NOT deduplicate across layers.

        The final result is sorted primarily by step_size ascending, then by the layer's lower bound ascending.
        """
        if not self.values_sorted or self.num_bins <= 0:
            return []

        all_layers = []  # We'll store each layer as a separate list

        for step_size in self.step_sizes:
            # ----------------------------------------------------
            # 1) Build base intervals for base_{step_size}
            # ----------------------------------------------------
            base_intervals: list[tuple[int | float | None, int | float | None, int, str]] = []
            left = self.min_value
            base_start_idx = step_size - 1
            if base_start_idx < len(self.edges):
                right = self.edges[base_start_idx]
                base_intervals.append((left, right, step_size, f"base_{step_size}"))
                left = right

                for i in range(base_start_idx + step_size, len(self.edges), step_size):
                    right = self.edges[i]
                    base_intervals.append((left, right, step_size, f"base_{step_size}"))
                    left = right

            # finalize base with [left, self.max_value]
            if left < self.max_value:
                base_intervals.append((left, self.max_value, step_size, f"base_{step_size}"))

            # open-end them
            base_intervals = self._finalize_open_ended(base_intervals)

            # store them as "one layer"
            all_layers.append(base_intervals)

            # ----------------------------------------------------
            # 2) If step_size >= 4, build offset_{step_size} as a separate layer
            # ----------------------------------------------------
            if step_size >= 4:
                offset_intervals = []
                num_offsets = 2
                offset_amount = step_size // num_offsets

                for offset_idx in range(1, num_offsets):
                    offset_start = offset_amount * offset_idx
                    if offset_start >= len(self.edges):
                        continue

                    left = self.min_value
                    right = self.edges[offset_start]
                    offset_intervals.append((left, right, step_size, f"offset_{step_size}"))
                    left = right

                    for i in range(offset_start + step_size, len(self.edges), step_size):
                        right = self.edges[i]
                        offset_intervals.append((left, right, step_size, f"offset_{step_size}"))
                        left = right

                    if left < self.max_value:
                        offset_intervals.append((left, self.max_value, step_size, f"offset_{step_size}"))

                # open-end them
                offset_intervals = self._finalize_open_ended(offset_intervals)

                # store them as another layer
                all_layers.append(offset_intervals)

        # ----------------------------------------------------
        # Flatten the layers into one final list
        # ----------------------------------------------------
        flattened = []
        for layer in all_layers:
            flattened.extend(layer)

        # ----------------------------------------------------
        # Sort the final list by (step_size, lower_bound)
        # ----------------------------------------------------
        def final_sort_key(itv):
            (low, high, sz, lbl) = itv

            # step_size ascending
            # label ascending as second key
            # lower bound ascending as last key

            # Handle 'low' depending on type
            if low is None:
                # Treat as negative infinity
                low_val = -1e18
            elif isinstance(low, (int, float)):
                # Convert to float
                low_val = float(low)
            elif isinstance(low, str):
                # If it's a string, use empty string as requested
                # But to avoid mixing str and float in the same key,
                # let's keep it as a separate "type layer."
                # We'll store a tuple: (2, '') for strings, (1, float) for numeric, etc.
                # For simplicity here, we’ll just store the empty string directly.
                # BUT we must ensure Python won’t compare '' with a float from other intervals,
                # so let's store a tuple (2, '') to consistently compare across intervals.
                low_val = (2, "")
            else:
                # Fallback for any other unexpected type
                # e.g. store as (3, string_representation)
                low_val = (3, str(low))

            # If numeric, store as (1, number) so we don't conflict with (2, '') above
            if isinstance(low_val, float):
                # float => (1, that_float)
                low_val = (1, low_val)

            # Now the final sort key is a tuple of:
            #   step_size, label, and (type_index, value)
            return (sz, lbl, low_val)

        flattened.sort(key=final_sort_key)
        #
        # # add a bucket that covers all values
        last_bucket = flattened[-1]
        # print(last_bucket)
        flattened.append((None, None, last_bucket[2] * 2, "N/A"))

        return flattened


# ---------------------------------------------------------------------
# Example Usage & Test Cases
# ---------------------------------------------------------------------
if __name__ == "__main__":
    print("=== EXAMPLE #1 ===")
    data = [1901, 1895, 2000, 1950, 1975, 1971, 2020, 2022, 2021, 1899, 1905]
    hbg = HierarchicalBucketGenerator(data, num_bins=4)
    intervals = hbg.generate_buckets()

    for low, high, step, label in intervals:
        print(f"[{low}, {high}] step={step} => {label}")
    print()

    print("=== EXAMPLE #2 ===")
    data2 = [10, 10, 12, 13, 50, 60, 60, 100, 100, 101]
    hbg2 = HierarchicalBucketGenerator(data2, num_bins=3)
    intervals2 = hbg2.generate_buckets()

    for low, high, step, label in intervals2:
        print(f"[{low}, {high}] step={step} => {label}")

    # -------------------------------------------------------------------------
    # EXAMPLE WITH STRINGS
    # -------------------------------------------------------------------------
    # We'll use some fruit names with duplicates.
    data_str = [
        "apple",
        "banana",
        "banana",
        "cherry",
        "date",
        "eggplant",
        "fig",
        "grape",
        "grape",
        "hazelnut",
        "kiwi",
        "kiwi",
        "lychee",
    ]

    # Let's pick num_bins=3 => next power of two is 4 => step sizes are 1, 2, 4
    hbg = HierarchicalBucketGenerator(data_str, num_bins=3)
    intervals = hbg.generate_buckets()

    print("=== HIERARCHICAL BUCKETS (STRINGS) ===")
    for low, high, step, label in intervals:
        print(f"[{low}, {high}] step={step} => {label}")
