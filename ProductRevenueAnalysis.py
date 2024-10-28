from mrjob.job import MRJob
from mrjob.step import MRStep
import csv

class ProductRevenueAnalysis(MRJob):

    # Dictionary to store product details for quick access
    product_data = {}

    def configure_args(self):
        super(ProductRevenueAnalysis, self).configure_args()
        self.add_file_arg('--products', help="Path to the products.csv file")

    def load_product_data(self):
        # Load product information from products.csv into a dictionary
        with open(self.options.products, 'r') as file:
            csv_reader = csv.reader(file)
            next(csv_reader)  # Skip header
            for record in csv_reader:
                prod_id, prod_name, category, cost = record[0], record[1], record[2], float(record[3])
                self.product_data[prod_id] = (prod_name, category, cost)

    def mapper_init(self):
        # Initialize product data on each mapper node
        self.load_product_data()

    def mapper_calculate_revenue(self, _, line):
        # Parse each line in transactions.csv and yield (ProductID, Revenue)
        try:
            fields = next(csv.reader([line]))
            if fields[0] != "TransactionID":  # Skip header
                prod_id = fields[3]
                revenue_amount = float(fields[5])
                yield prod_id, revenue_amount
        except Exception as error:
            pass  # Handle parsing errors

    def reducer_total_revenue(self, prod_id, revenue_stream):
        # Calculate total revenue for each product
        total_revenue = sum(revenue_stream)
        if prod_id in self.product_data:
            prod_name, category, cost = self.product_data[prod_id]
            yield category, (prod_name, total_revenue, cost)

    def reducer_top_three_products(self, category, product_list):
        # Determine top 3 most profitable products by total revenue
        top_products = sorted(product_list, key=lambda x: x[1], reverse=True)[:3]
        for product in top_products:
            yield category, product

    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_init,
                   mapper=self.mapper_calculate_revenue,
                   reducer=self.reducer_total_revenue),
            MRStep(reducer=self.reducer_top_three_products)
        ]

if __name__ == '__main__':
    ProductRevenueAnalysis.run()