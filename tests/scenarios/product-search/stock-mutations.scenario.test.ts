import { describe, it } from "vitest";
import { buildProductCatalogFixture } from "../helpers/api-fixtures";
import { expectMutationResult, expectRowsEqual } from "../helpers/assertions";
import { expectDirtyIndex } from "../helpers/explain";
import { deleteFromOracle, updateOracle } from "../helpers/oracle";

describe("product search stock mutation endpoint scenarios", () => {
  it("PATCH /products/restock updates low-stock active products and preserves range query parity", () => {
    const { products, oracle } = buildProductCatalogFixture();

    const updated = updateOracle(
      oracle,
      (row) => row.status === "active" && row.stock < 5,
      { stock: 50 },
    );
    expectMutationResult(
      products.updateMany({ status: "active", stock: { lt: 5 } }, { stock: 50 }),
      updated,
    );

    const query = products.where("stock", "<", 5);
    expectDirtyIndex(query, "sorted:stock");
    expectRowsEqual(
      query.toArray(),
      oracle.filter((row) => row.stock < 5),
    );
  });

  it("DELETE /products?status=discontinued removes inactive catalog rows and keeps active search correct", () => {
    const { products, oracle } = buildProductCatalogFixture();

    const deleted = deleteFromOracle(oracle, (row) => row.status === "discontinued");
    expectMutationResult(products.deleteMany({ status: "discontinued" }), deleted);

    const query = products.where({ category: "books", status: "active" });
    expectDirtyIndex(query, "equality:category");
    expectRowsEqual(
      query.toArray(),
      oracle.filter((row) => row.category === "books" && row.status === "active"),
    );
  });
});
