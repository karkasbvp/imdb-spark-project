
def main():
    spark_session = ...

    novies_df = spark_session.csv(path)
    novies_df.show()
if __name__ == "__main__":
    main()