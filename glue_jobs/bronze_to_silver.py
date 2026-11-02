"""AWS Glue entrypoint. lakehouse_src.zip is passed with --extra-py-files."""
from lakehouse.spark.bronze_to_silver import main

if __name__ == "__main__":
    main()
