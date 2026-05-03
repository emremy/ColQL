import { describe, it } from "vitest";
import { buildProductCatalogFixture } from "../helpers/api-fixtures";
import { expectRowsEqual } from "../helpers/assertions";
import { expectDirtyIndex, expectFreshIndex } from "../helpers/explain";
import { updateOracle } from "../helpers/oracle";

describe("consistency dirty index lifecycle endpoint scenarios", () => {
  it("PATCH /products dirty indexes explain before execution and become fresh after requery", () => {
    const { products, oracle } = buildProductCatalogFixture();

    updateOracle(
      oracle,
      (row) => row.category === "games" && row.price >= 20_000,
      { status: "inactive" },
    );
    products.updateMany(
      { category: "games", price: { gte: 20_000 } },
      { status: "inactive" },
    );

    const statusQuery = products.where("status", "=", "inactive");
    expectDirtyIndex(statusQuery, "equality:status");
    expectRowsEqual(
      statusQuery.toArray(),
      oracle.filter((row) => row.status === "inactive"),
    );
    expectFreshIndex(products.where("status", "=", "inactive"), "equality:status");

    const priceQuery = products.where("price", ">=", 49_000);
    expectDirtyIndex(priceQuery, "sorted:price");
    expectRowsEqual(
      priceQuery.toArray(),
      oracle.filter((row) => row.price >= 49_000),
    );
    expectFreshIndex(products.where("price", ">=", 49_000), "sorted:price");
  });
});
