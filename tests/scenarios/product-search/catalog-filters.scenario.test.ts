import { describe, it } from "vitest";
import { buildProductCatalogFixture } from "../helpers/api-fixtures";
import { expectProjectedRows } from "../helpers/assertions";
import { expectProjectionPushdown, expectUsesIndex } from "../helpers/explain";
import { projectRows } from "../helpers/oracle";

describe("product search catalog filter endpoint scenarios", () => {
  it("GET /products?category=tools&status=active uses equality index and projects results", () => {
    const { products, oracle } = buildProductCatalogFixture();

    const query = products
      .where({ category: "tools", status: "active" })
      .select(["id", "price", "stock"])
      .limit(50);
    const expected = projectRows(
      oracle
        .filter((row) => row.category === "tools" && row.status === "active")
        .slice(0, 50),
      ["id", "price", "stock"],
    );

    expectUsesIndex(query, "equality:category");
    expectProjectionPushdown(query);
    expectProjectedRows(query.toArray(), expected);
  });
});
