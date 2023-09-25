# Code for custom code recipe MERGE (imported from a Python recipe)

# To finish creating your custom recipe from your original PySpark recipe, you need to:
#  - Declare the input and output roles in recipe.json
#  - Replace the dataset names by roles access in your code
#  - Declare, if any, the params of your custom recipe in recipe.json
#  - Replace the hardcoded params values by acccess to the configuration map

# See sample code below for how to do that.
# The code of your original recipe is included afterwards for convenience.
# Please also see the "recipe.json" file for more information.

# # import the classes for accessing DSS objects from the recipe
# import dataiku
# # Import the helpers for custom recipes
# from dataiku.customrecipe import *

# # Inputs and outputs are defined by roles. In the recipe's I/O tab, the user can associate one
# # or more dataset to each input and output role.
# # Roles need to be defined in recipe.json, in the inputRoles and outputRoles fields.

# # To  retrieve the datasets of an input role named 'input_A' as an array of dataset names:
# input_A_names = get_input_names_for_role('input_A_role')
# # The dataset objects themselves can then be created like this:
# input_A_datasets = [dataiku.Dataset(name) for name in input_A_names]

# # For outputs, the process is the same:
# output_A_names = get_output_names_for_role('main_output')
# output_A_datasets = [dataiku.Dataset(name) for name in output_A_names]


# # The configuration consists of the parameters set up by the user in the recipe Settings tab.

# # Parameters must be added to the recipe.json file so that DSS can prompt the user for values in
# # the Settings tab of the recipe. The field "params" holds a list of all the params for wich the
# # user will be prompted for values.

# # The configuration is simply a map of parameters, and retrieving the value of one of them is simply:
# my_variable = get_recipe_config()['parameter_name']

# # For optional parameters, you should provide a default value in case the parameter is not present:
# my_variable = get_recipe_config().get('parameter_name', None)

# Note about typing:
# The configuration of the recipe is passed through a JSON object
# As such, INT parameters of the recipe are received in the get_recipe_config() dict as a Python float.
# If you absolutely require a Python int, use int(get_recipe_config()["my_int_param"])


#############################
# Your original recipe
#############################


#    _____ __  __ _____   ____  _____ _______ _____
#   |_   _|  \/  |  __ \ / __ \|  __ \__   __/ ____|
#     | | | \  / | |__) | |  | | |__) | | | | (___
#     | | | |\/| |  ___/| |  | |  _  /  | |  \___ \
#    _| |_| |  | | |    | |__| | | \ \  | |  ____) |
#   |_____|_|  |_|_|     \____/|_|  \_\ |_| |_____/
#
import dataiku
import pandas as pd
import numpy as np
from dataiku import pandasutils as pdu
from dataiku import SQLExecutor2
# Import the helpers for custom recipes
from dataiku.customrecipe import *
import datetime


#     _____ ______ _______   _____  ______ _____ _____ _____  ______   _____ _   _ _____  _    _ _______ _____
#    / ____|  ____|__   __| |  __ \|  ____/ ____|_   _|  __ \|  ____| |_   _| \ | |  __ \| |  | |__   __/ ____|
#   | |  __| |__     | |    | |__) | |__ | |      | | | |__) | |__      | | |  \| | |__) | |  | |  | | | (___
#   | | |_ |  __|    | |    |  _  /|  __|| |      | | |  ___/|  __|     | | | . ` |  ___/| |  | |  | |  \___ \
#   | |__| | |____   | |    | | \ \| |___| |____ _| |_| |    | |____   _| |_| |\  | |    | |__| |  | |  ____) |
#    \_____|______|  |_|    |_|  \_\______\_____|_____|_|    |______| |_____|_| \_|_|     \____/   |_| |_____/
#


# Get input and output roles
# https://developer.dataiku.com/latest/api-reference/python/plugin-components/custom_recipes.html
target_dataset_name = get_input_names_for_role('target_dataset')[0]
source_dataset_name = get_input_names_for_role('source_dataset')[0]
merge_output_name = get_output_names_for_role('merge_output')[0]

# Get target and source datasets
target_dataset = dataiku.Dataset(target_dataset_name)
source_dataset = dataiku.Dataset(source_dataset_name)

# Get target and source datasets (dataikuapi alternative)
# https://developer.dataiku.com/latest/api-reference/python/datasets.html#dataikuapi.dss.dataset.DSSDataset.get_info
#
# client = dataiku.api_client()
# project = client.get_default_project()
# target_dataset_api = project.get_dataset(target_dataset_name.replace(
#     project.get_summary()['projectKey'] + '.', '', 1))
# source_dataste_api = project.get_dataset(source_dataset_name.replace(
#     project.get_summary()['projectKey'] + '.', '', 1))

# Get location info for target and source datasets
# https://developer.dataiku.com/latest/api-reference/python/datasets.html#dataiku.Dataset.get_location_info
target_dataset_location_info = target_dataset.get_location_info()
source_dataset_location_info = source_dataset.get_location_info()

# Get connection name for target and source datasets
target_dataset_connection_name = target_dataset_location_info["info"]["connectionName"]
source_dataset_connection_name = source_dataset_location_info["info"]["connectionName"]

# Get connection type for target and source datasets
target_dataset_connection_type = target_dataset_location_info["info"]["databaseType"]
source_dataset_connection_type = source_dataset_location_info["info"]["databaseType"]

# Get SQL table names for target and source datasets
# https://community.dataiku.com/t5/Using-Dataiku/List-of-all-project-and-respective-tables-in-dataiku/m-p/23480
target_dataset_table_name = target_dataset_location_info["info"]["table"]
source_dataset_table_name = source_dataset_location_info["info"]["table"]

# Get the params
sql_database_engine = get_recipe_config()['sql_database_engine']
target_dataset_match_keys_list = get_recipe_config()[
    'target_dataset_match_keys']
source_dataset_match_keys_list = get_recipe_config()[
    'source_dataset_match_keys']

# # Get SQL table column names for target and source datasets
target_dataset_column_names_list = list(
    map(lambda feature: feature["name"], target_dataset.read_schema()))
source_dataset_column_names_list = list(
    map(lambda feature: feature["name"], source_dataset.read_schema()))

# Get SQL table full column names (with table name prefix) for source and target datasets
source_dataset_full_column_names_list = ['"{0}"."{1}"'.format(
    source_dataset_table_name, i) for i in source_dataset_column_names_list]
target_dataset_full_column_names_list = ['"{0}"."{1}"'.format(
    source_dataset_table_name, i) for i in target_dataset_column_names_list]


#    __  __ ______ _____   _____ ______    _____  ____  _         ____  _    _ ______ _______     __
#   |  \/  |  ____|  __ \ / ____|  ____|  / ____|/ __ \| |       / __ \| |  | |  ____|  __ \ \   / /
#   | \  / | |__  | |__) | |  __| |__    | (___ | |  | | |      | |  | | |  | | |__  | |__) \ \_/ /
#   | |\/| |  __| |  _  /| | |_ |  __|    \___ \| |  | | |      | |  | | |  | |  __| |  _  / \   /
#   | |  | | |____| | \ \| |__| | |____   ____) | |__| | |____  | |__| | |__| | |____| | \ \  | |
#   |_|__|_|______|_|__\_\\_____|______|_|_____/ \___\_\______|__\___\_\\____/|______|_|  \_\ |_|__
#   |  _ \| |  | |_   _| |    |  __ \  |  ____| |  | | \ | |/ ____|__   __|_   _/ __ \| \ | |/ ____|
#   | |_) | |  | | | | | |    | |  | | | |__  | |  | |  \| | |       | |    | || |  | |  \| | (___
#   |  _ <| |  | | | | | |    | |  | | |  __| | |  | | . ` | |       | |    | || |  | | . ` |\___ \
#   | |_) | |__| |_| |_| |____| |__| | | |    | |__| | |\  | |____   | |   _| || |__| | |\  |____) |
#   |____/ \____/|_____|______|_____/  |_|     \____/|_| \_|\_____|  |_|  |_____\____/|_| \_|_____/
#
#    ___  ___  ___ _____ ___ ___ ___ ___  ___  _
#   | _ \/ _ \/ __|_   _/ __| _ \ __/ __|/ _ \| |
#   |  _/ (_) \__ \ | || (_ |   / _|\__ \ (_) | |__
#   |_|  \___/|___/ |_| \___|_|_\___|___/\__\_\____|
#
def build_postgresql_merge_sql_query():
    """Build and return MERGE/UPSERT SQL query for PostgreSQL"""

    # https://www.postgresql.org/docs/current/sql-insert.html
    # https://dba.stackexchange.com/questions/134493/upsert-with-on-conflict-using-values-from-source-table-in-the-update-part
    # https://stackoverflow.com/questions/48922972/postgres-insert-on-conflict-do-update-vs-insert-or-update
    # https://stackoverflow.com/questions/51375439/postgres-on-conflict-do-update-on-composite-primary-keys

    # Template:
    # INSERT INTO target (target_column_1, target_column_2, ..., target_column_n)
    # SELECT source_column_1, source_column_2, ..., source_column_n
    # FROM   source
    # ON     CONFLICT (target_match_key_1, target_match_key_2, ..., target_match_key_n) DO UPDATE -- conflict is on the unique column
    # SET    target_column_1 = EXCLUDED.target_column_1,
    #        target_column_2 = EXCLUDED.target_column_2
    #        target_column_n = EXCLUDED.target_column_n; -- key word "excluded", refer to target column

    # Get SQL "SET" statement clause RE: target dataset
    target_dataset_set_statement_list = ['{0} = EXCLUDED.{0}'.format(
        i) for i in target_dataset_column_names_list if i not in target_dataset_match_keys_list]

    sql_query_list = [
        "INSERT INTO",
        '"{0}"'.format(target_dataset_table_name),
        "({0})".format(", ".join(target_dataset_column_names_list)),
        "\nSELECT",
        ", ".join(source_dataset_column_names_list),
        "\nFROM",
        '"{0}"'.format(source_dataset_table_name),
        "\nON CONFLICT",
        '({0})'.format(", ".join(target_dataset_match_keys_list)),
        "\nDO UPDATE",
        "\nSET\n",
        ",\n".join(target_dataset_set_statement_list)
    ]

    sql_query_string = " ".join(sql_query_list)
    return sql_query_string


#     ___  ___    _   ___ _    ___
#    / _ \| _ \  /_\ / __| |  | __|
#   | (_) |   / / _ \ (__| |__| _|
#    \___/|_|_\/_/ \_\___|____|___|
#
def build_oracle_database_merge_sql_query():
    """Build and return MERGE/UPSERT SQL query for Oracle Database"""

    # https://docs.oracle.com/en/database/oracle/oracle-database/12.2/sqlrf/MERGE.html#GUID-5692CCB7-24D9-4C0E-81A7-A22436DC968F

    sql_query_list = [
        "INSERT INTO",
        '"{0}"'.format(target_dataset_table_name),
        "({0})".format(", ".join(target_dataset_column_names_list)),
        "\nSELECT",
        ", ".join(source_dataset_column_names_list),
        "\nFROM",
        '"{0}"'.format(source_dataset_table_name),
        "\nON CONFLICT",
        '({0})'.format(target_dataset_match_keys_list),
        "\nDO UPDATE",
        "\nSET\n",
        ",\n".join(target_dataset_set_statement_list)
    ]

    sql_query_string = " ".join(sql_query_list)
    return sql_query_string


#    __  __ ___   ___  ___  _      ___ ___ _____   _____ ___
#   |  \/  / __| / __|/ _ \| |    / __| __| _ \ \ / / __| _ \
#   | |\/| \__ \ \__ \ (_) | |__  \__ \ _||   /\ V /| _||   /
#   |_|  |_|___/ |___/\__\_\____| |___/___|_|_\ \_/ |___|_|_\
#
def build_microsoft_sql_server_merge_sql_query():
    """Build and return MERGE/UPSERT SQL query for Microsoft SQL Server"""
    return ''


#      _   __  __   _    _______  _  _   ___ ___ ___  ___ _  _ ___ ___ _____
#     /_\ |  \/  | /_\  |_  / _ \| \| | | _ \ __|   \/ __| || |_ _| __|_   _|
#    / _ \| |\/| |/ _ \  / / (_) | .` | |   / _|| |) \__ \ __ || || _|  | |
#   /_/ \_\_|  |_/_/ \_\/___\___/|_|\_| |_|_\___|___/|___/_||_|___|_|   |_|
#
def build_amazon_redshift_merge_sql_query():
    """Build and return MERGE/UPSERT SQL query for Amazon Redshift"""
    return ''


#    ________   ________ _____ _    _ _______ ______    _____  ____  _         ____  _    _ ______ _______     __
#   |  ____\ \ / /  ____/ ____| |  | |__   __|  ____|  / ____|/ __ \| |       / __ \| |  | |  ____|  __ \ \   / /
#   | |__   \ V /| |__ | |    | |  | |  | |  | |__    | (___ | |  | | |      | |  | | |  | | |__  | |__) \ \_/ /
#   |  __|   > < |  __|| |    | |  | |  | |  |  __|    \___ \| |  | | |      | |  | | |  | |  __| |  _  / \   /
#   | |____ / . \| |___| |____| |__| |  | |  | |____   ____) | |__| | |____  | |__| | |__| | |____| | \ \  | |
#   |______/_/ \_\______\_____|\____/   |_|  |______| |_____/ \___\_\______|  \___\_\\____/|______|_|  \_\ |_|
#
# Execute SQL query against target dataset
#
# print("-------------------- RUNNING THE FOLLOWING SQL QUERY --------------------")
# print("-------------------- -------------------------------- --------------------")
# print("-------------------- -------------------------------- --------------------")
# print(sql_query_string)
# print("-------------------- -------------------------------- --------------------")
# print("-------------------- -------------------------------- --------------------")
# print("-------------------- -------------------------------- --------------------")
#
# executor = SQLExecutor2(dataset=target_dataset)
# output_df = executor.query_to_df(sql_query_string, post_queries=['COMMIT'])
# output_df = pd.DataFrame({"executed_sql_query": [sql_query_string],
#                          "timestamp": [datetime.datetime.now()]})
#
# # Compute a Pandas dataframe to write into merge_output
# merge_output_df = output_df
#
# # Write recipe outputs
# merge_output = dataiku.Dataset(merge_output_name)
# merge_output.write_with_schema(merge_output_df)
