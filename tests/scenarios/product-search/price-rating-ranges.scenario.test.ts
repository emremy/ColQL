import { describe, it } from "vitest";
import { buildProductCatalogFixture } from "../helpers/api-fixtures";
import { expectRowsEqual } from "../helpers/assertions";
import { expectUsesIndex } from "../helpers/explain";

describe("product search price and rating range endpoint scenarios", () => {
  it("GET /products?priceMin=&priceMax=&rating>= uses sorted price index and matches oracle", () => {
    const { products, oracle } = buildProductCatalogFixture();
    const minPrice = 10_000;
    const maxPrice = 12_500;

    const query = products.where({
      price: { gte: minPrice, lte: maxPrice },
      rating: { gte: 4 },
      status: "active",
    });
    const expected = oracle.filter(
      (row) =>
        row.price >= minPrice &&
        row.price <= maxPrice &&
        row.rating >= 4 &&
        row.status === "active",
    );

    expectUsesIndex(query, "sorted:price");
    expectRowsEqual(query.toArray(), expected);
  });
});
