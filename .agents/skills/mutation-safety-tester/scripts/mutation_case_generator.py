scenarios = [
    "delete_first_row",
    "delete_last_row",
    "delete_middle_row",
    "delete_consecutive_rows",
    "delete_across_chunk_boundary",
    "delete_zero_percent",
    "delete_one_percent",
    "delete_fifty_percent",
    "delete_ninety_percent",
    "update_indexed_column",
    "update_non_indexed_column",
    "update_unique_indexed_column",
    "update_then_delete",
    "delete_then_update",
    "invalid_update_atomicity",
    "duplicate_unique_key_atomicity",
    "update_by_existing_key",
    "delete_by_existing_key",
    "by_key_missing_unique_index",
]

for scenario in scenarios:
    print(f"- [ ] {scenario}")
