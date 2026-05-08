from itertools import product

operators = ["eq", "in", "gt", "gte", "lt", "lte", "findBy", "updateBy", "deleteBy"]
mutation_states = ["clean", "after_update", "after_delete"]
value_shapes = ["unique", "duplicates", "missing", "boundary", "dirty_unique"]

for operator, mutation_state, value_shape in product(
    operators,
    mutation_states,
    value_shapes,
):
    print(f"{operator} | {mutation_state} | {value_shape}")
