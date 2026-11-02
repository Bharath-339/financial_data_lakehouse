"""AWS Glue entrypoint. lakehouse_src.zip is passed with --extra-py-files."""
from lakehouse.spark.silver_to_gold import main

if __name__ == "__main__":
    main()
