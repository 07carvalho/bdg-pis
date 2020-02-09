import argparse
import csv
import re
from pyspark.sql import SparkSession
from pyspark.sql import Row


products_file_name = 'products.csv'
reviews_file_name = 'reviews.csv'


def do_csv(file_path):
    products_file = open(products_file_name, 'w')
    products_writer = csv.writer(products_file)
    products_writer.writerow(['product_id', 'asin', 'group', 'salesrank'])

    reviews_file = open(reviews_file_name, 'w')
    reviews_writer = csv.writer(reviews_file)
    reviews_writer.writerow(['product_id', 'date', 'cutomer', 'rating', 'votes', 'helpful'])

    with open(file_path) as fp: 
        lines = fp.readlines()
        product_id = None
        asin = None
        title = None
        group = None
        salesrank = None
        for line in lines:
            if 'Id:' in line:
                # this is the first line of a product
                product_id = re.search(r"Id:(.*)\n", line).group(1).strip()
            elif 'ASIN:' in line:
                asin = re.search(r"ASIN:(.*)\n", line).group(1).strip()
            elif 'group:' in line:
                group = re.search(r"group:(.*)\n", line).group(1).strip()
            elif 'salesrank:' in line:
                salesrank = re.search(r"salesrank:(.*)\n", line).group(1).strip()
                product_dict = [product_id, asin, group, salesrank]
                products_writer.writerow(product_dict)
            elif 'cutomer:' in line:
                fields = [i for i in line.split(' ') if i != ' ' and i != '']
                review_dict = [product_id, fields[0], fields[2], fields[4], fields[6], re.sub('\s+', '', fields[8])]
                reviews_writer.writerow(review_dict)


def load_df():
    spark = SparkSession.builder.appName("Amazon Meta").getOrCreate()

    # load the products data
    dfProducts = spark.read.format('com.databricks.spark.csv').option('header', 'true').option('inferSchema', 'true').load(products_file_name)
    dfProducts.createOrReplaceTempView('product')

    # load the reviews data
    dfReviews = spark.read.format('com.databricks.spark.csv').option('header', 'true').option('inferSchema', 'true').load(reviews_file_name)
    dfReviews.createOrReplaceTempView('review')

    # 1.a) Given a product, list the 5 most helpful and highest rated reviews
    sqlDF1 = spark.sql("SELECT * FROM review WHERE product_id=7157 ORDER BY helpful DESC, rating DESC LIMIT 5")
    print(sqlDF1.show())

    # 1.b) Given a product, list the 5 most helpful and lowest rated reviews
    sqlDF2 = spark.sql("SELECT * FROM review WHERE product_id=7157 ORDER BY helpful DESC, rating ASC LIMIT 5")
    print(sqlDF2.show())

    # 7) List the 10 customers that most commented by product group
    group_list = dfProduct.select("group").distinct().collect()
    for group in group_list:
        name = str(group.__getitem__("group"))
        sql_reviewers = spark.sql("SELECT cutomer, COUNT(cutomer) AS counter FROM review r LEFT JOIN product p on r.product_id = p.product_id WHERE p.group = '" + name + "' GROUP BY cutomer ORDER BY counter DESC LIMIT 10")
        sql_reviewers.show()

    spark.stop()


def main(file_path):
    print('hjdshdj')
    do_csv(file_path)
    load_df()
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='A tutorial of argparse!')
    parser.add_argument("--file", required=True, type=str, help='File path')
    args = parser.parse_args()
    file_path = args.file
    # parser.add_argument("--a", default=1, type=int, help="This is the 'a' variable")
    main(file_path)
