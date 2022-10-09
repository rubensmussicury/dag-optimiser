# Copyright 2022 Rubens Mussi Cury – rubensmussicury@gmail.com
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import re
import argparse
import os
import sql_metadata
import pandas as pd
import json
from itertools import groupby
from graphviz import Digraph

# GLOBAL VARIABLES
# Store current working directory.
CURRENT_DIR = os.getcwd()

# Store DAGs SQL files described in .py
# "bash_command" but not found.
FILES_NOT_FOUND = []

# Store affected tables existent in SQL files without FROM
# (Ex.: DELETE AFFECTED_NO_FROM WHERE TRUE).
AFFECTED_NO_FROM = []

# Store all affected tables and their respective types
# {"sql_type": sql_type, "affected_table": affected_table}.
AFFECTED_TABLES = []

# Store all affected tables and their respective types
# {"sql_type": sql_type, "affected_table": affected_table}.
SQL_TYPE_NOT_FOUND = []


# Read SQL Files
def remove_sql_comments(raw_sql):
    """
    Remove out all types of SQL comments ("#", "--", "/* */").
    :param raw_sql: Raw SQL string.
    :return: The new SQL string without all comments ("#", "--", "/* */") keeping initial
    sql aesthetics like break lines, spaces and tabulation.
    """

    # List to be used to store the clean SQL.
    clean_sql = []

    # Split raw sql in lines.
    sql_lines = raw_sql.splitlines()

    # By default, set clean next line as False.
    clean_next_line = False

    # Loop all SQL lines.
    for sql_line in sql_lines:

        # Remove left and right spaces.
        clean_line = sql_line.strip()

        # When "clean_next_line" is True means that the string "/*" was found previously.
        # But the whole line will only be ignored if there is no "*/" in it also. Otherwise,
        # he line will be kept in order to consider any text after "*/" in next steps.
        if clean_next_line and clean_line.find("*/") == -1:
            clean_line = ""

        if clean_line:
            # Check if the string "/*" exists.
            bar_start_found = clean_line.find("/*")
            if bar_start_found >= 0:

                bar_end_found = clean_line.find("*/")
                if bar_end_found == -1:
                    # Mark next line to be ignored.
                    clean_next_line = True

                # Get all the text BEFORE the "/*".
                clean_line = clean_line[:bar_start_found]

            # Check if the string "*/" exists.
            bar_end_found = clean_line.find("*/")
            if bar_end_found >= 0:
                # Get all the text AFTER the "*/".
                clean_line = clean_line[bar_end_found + 2:]
                # Handle next line normally.
                clean_next_line = False

            # Check if the string "--" exists.
            dash_found = clean_line.find("--")
            if dash_found >= 0:
                # Get all the text BEFORE the "--".
                clean_line = clean_line[:dash_found]

            # Check if the string "#" exists.
            hash_found = clean_line.find("#")
            if hash_found >= 0:
                # Get all the text BEFORE the "#".
                clean_line = clean_line[:hash_found]

            # Ensures that text content is left.
            if len(clean_line) > 0:
                # Append the line with all comments removed out.
                clean_sql.append(clean_line)

    return "\n".join(clean_sql)


def adjust_sql(raw_sql, start_line=0):
    """
    Make some adjusts in raw SQL in order to process rightly using sqlparse library.
    :param raw_sql: Raw SQL string.
    :param start_line: The line to start.
    :return: Adjusted raw SQL
    """

    # Indicate that SQL was not changed.
    changed = False

    # Texts to be replaced otherwise the followed tables will not be recognized.
    combinations_to_replace = {"left outer join": "left join",
                               "right outer join": "right join",
                               "full outer join": "join"}

    # Replace join types ignoring case.
    for from_text, to_text in combinations_to_replace.items():
        raw_sql = re.sub(from_text, to_text, raw_sql, flags=re.I)

    # Remove "TAB" and "BREAK LINE" and capitalize to UPPER.
    compressed_sql = raw_sql.replace("\n", " {BREAK_LINE} ").replace("\t", "  ").upper()

    # Split string by " ".
    sql_parts = compressed_sql.split(" ")

    # Remove spaces
    sql_parts = [line for line in sql_parts if line.strip() != '']

    # Set from found as false
    from_found = False

    # From found limit.
    from_found_limit = 0

    detected_tables = sql_metadata.get_query_tables(raw_sql)
    for index_part, sql_part in enumerate(sql_parts[start_line:], start=start_line):

        # If part contains "FROM"
        if (sql_part == "FROM" or from_found) and from_found_limit <= 1:

            if len(sql_parts) - 1 > index_part:
                # Get the next text right after "FROM" and remove ","
                next_part = sql_parts[index_part + 1].replace(",", "")
            else:
                break

            # If the content represents a "{BREAK_LINE}", ignores it but "from_found" as true.
            if next_part == "{BREAK_LINE}":
                from_found = True
                continue

            # Use this counter to ensure that even after finding a "FROM", this looping not occur if
            # "," is not found in the next 2 lines, indicating we are at the last "FROM".

            #     FROM
            #       XPT.TABLE1 E,
            #       XYZ.TABLE3 V (this code guarantees this loop not occurs after "V" was fount without finding ",")
            elif from_found:
                from_found_limit += 1

            # If next part is a table.
            if next_part in detected_tables:

                # Check if "," exists in string.
                if "," in next_part:
                    # Replace "," by " FROM" in order to fix the bug.
                    sql_parts[index_part + 1] = sql_parts[index_part + 1].replace(",", " FROM")
                    changed = True
                    break

                if index_part + 2 <= len(sql_parts) - 1:
                    # In case there is a alias after the table like FROM TABLE T,
                    if "," in sql_parts[index_part + 2] and sql_parts[index_part + 1] in detected_tables:
                        # Replace "," by " FROM" in order to fix the bug.
                        sql_parts[index_part + 2] = sql_parts[index_part + 2].replace(",", " FROM")
                        changed = True
                        break

                if index_part + 3 <= len(sql_parts) - 1:
                    # In case there is an "AS" after the table like FROM TABLE AS T,
                    if "," in sql_parts[index_part + 3] and "AS" in sql_parts[index_part + 2]:
                        # Replace "," by " FROM" in order to fix the bug.
                        sql_parts[index_part + 3] = sql_parts[index_part + 3].replace(",", " FROM")
                        changed = True
                        break

                if changed:
                    # Reset "from_found" to False and "from_found_limit" to 0.
                    from_found = False
                    from_found_limit = 0

    adjusted_sql = " ".join(sql_parts)
    if changed:
        # Call this function again.
        return adjust_sql(adjusted_sql, index_part + 1)
    else:
        # Join all entries again keeping the initially arrangement.
        return "\n".join(adjusted_sql.split("{BREAK_LINE}"))


def get_target_table(raw_sql, tables_list):
    """
    Find the name o the table in which has any kind of changer instruction like CREATE, INSERT, etc.
    :param tables_list: Full table list found by the sql_metadata
    :param raw_sql: Raw SQL string.
    :return: The real name of the table or "no target table" in case we have a simple SELECT for example.
    """
    # Set all possible changer instructions types.
    changer_types = ["CREATE", "INSERT", "DELETE", "UPDATE"]

    # Get the instruction type used in sql like (SELECT, CREATE, ETC).
    adjusted_sql = raw_sql.replace("\n", " ").replace("\t", "  ").upper().split(" ")

    # Return no target table in case the instruction is not a changer, like "SELECT" for example or empty raw_sql.
    affected_table = "NONE_AFFECTED_TABLE"

    # Remove empty entries.
    adjusted_sql = [line for line in adjusted_sql if line.strip() != '']

    if len(adjusted_sql) > 0:
        # Get SQL type.
        sql_type = adjusted_sql[0]

        # Check if the SQL type is any of ones changer types set.
        if any(n in sql_type for n in changer_types):
            # Ensure there is at least one table defined.
            if len(tables_list) > 0:
                # Workaround to bring the affected table if the type of syntax is "DELETE" without "FROM"
                if sql_type == "DELETE":
                    # Check the right next word, after DELETE. If it is not "FROM", add it.
                    if adjusted_sql[1] != "FROM":
                        # Replace DELETE by DELETE FROM, then "FROM * FROM by FROM in case initial syntax
                        # was DELETE * FROM avoiding to get a final result as DELETE FROM * FROM
                        raw_sql = raw_sql.replace("DELETE", "DELETE FROM", 1).replace("FROM * FROM", "FROM")
                        tables_list = sql_metadata.get_query_tables(raw_sql)

                affected_table = tables_list[0]

                # Increment AFFECTED_TABLES global variable.
                AFFECTED_TABLES.append({"sql_type": sql_type, "affected_table": affected_table})
            else:
                affected_table = adjusted_sql[1]
                # Increment AFFECTED_NO_FROM global variable.
                AFFECTED_NO_FROM.append({"sql_type": sql_type, "affected_table": affected_table})
    else:
        affected_table = "NO_SQL_TYPE_FOUND"

    return affected_table


def get_sql_details(raw_sql, dag_id, dag_name, task_id):
    """
    Get JSON string containing the task name, target table, from table(s), join table(s)
    :param raw_sql: Raw SQL string to be parsed.
    :param dag_id: DAG Identification.
    :param dag_name: DAG Name.
    :param task_id: The name of task.
    :return: JSON
                {'task_id': {'target_table': 'EDW.TMP_STORE_SE',
                  'from_join_tables': ['DATACENTERWM.CONVERSAOLOJAS',
                   'EDW.TBL_ULTIMA_CARGA',
                   'DATACENTERWM.FILIAL'],
                  'dag_name': 'dag_name',
                  'dag_id': 'dag_id'}}
    """
    # Remove all comments from SQL
    no_comments_sql = remove_sql_comments(raw_sql)

    # Adjust raw SQL.
    raw_sql = adjust_sql(no_comments_sql)

    # Check all statement. Every ";" found indicates a new statement. No ";" means there it's unique.
    task_details = {task_id: {}}

    # Get all tables mentioned in SQL clause.
    all_tables_list = sql_metadata.get_query_tables(raw_sql)

    # Uncomment this lines to show DAG ID and TASK ID for each table found.
    # for table_index, table in enumerate(all_tables_list):
    #    all_tables_list[table_index] = "{0}  DAG_ID:{1}   TASK_ID:{2}".format(table, dag_id, task_id)

    # Get target table - the table that is changed.
    target_table = get_target_table(raw_sql, all_tables_list)

    # In case none SQL type was identified, store it.
    if target_table == "NO_SQL_TYPE_FOUND":
        SQL_TYPE_NOT_FOUND.append("DAG_ID: {0}  -  TASK_ID: {1}".format(dag_id, task_id))
    else:
        # Uncomment this lines to show DAG ID and TASK ID for each target table found.
        # target_table =  "{0}  DAG_ID:{1}   TASK_ID:{2}".format(target_table, dag_id, task_id)

        # Set a fake name to the "target_table" and replace the current one by the fake one
        # as a workaround when the the affected table is also used as a reading table.
        # Ex. DELETE FROM XYZ.TABLE1 AS T1 WHERE T1.ID_CUSTOMER = (SELECT T2.ID_SUPPLIER FROM XYZ.TABLE1 AS T2)
        target_table_fake = target_table + "_99_REMOVE_99"
        raw_sql = raw_sql.replace(target_table, target_table_fake, 1)

        # Get all tables mentioned in SQL clause again, but from now on, repeated tables
        # (affected and reading) will be both considered - once they will not have the same name anymore.
        all_tables_list = sql_metadata.get_query_tables(raw_sql)

        # Remove the fake affected table (if existent) from the all tables list.
        if target_table_fake in all_tables_list:
            all_tables_list.remove(target_table_fake)

    task_details[task_id] = {}
    task_details[task_id]["dag_id"] = dag_id
    task_details[task_id]["dag_name"] = dag_name
    task_details[task_id]["target_table"] = [target_table]
    task_details[task_id]["from_join_tables"] = all_tables_list

    return task_details


# Find DAGs Relations
def flat_list(multilevel_list):
    """
    Flat a multilevel list.
    :param multilevel_list: Any kind of list.
    :return: Flattern list.
    """
    flattern_list = []

    for element in multilevel_list:
        # If element is a list, call this function again.
        if isinstance(element, list):
            flattern_list += flat_list(element)
        else:
            # If element is a number or string, simply append it.
            if isinstance(element, int) or isinstance(element, str):
                flattern_list.append(element)

    return flattern_list


def get_reading_table_dag_relations(dags, current_index, current_reading_table, search_type, keep_last=True):
    """
    Return all DAGs in which contains on it's target tables the reading table.
    :param dags: JSON of dags in this format -> [dag_id, dag_name, qty_read_tasks, target_tables, from_join_tables]
    :param current_index: Current position of cursor in DAGs list in order to
    :param current_reading_table: Current reading table.
    :param search_type: BEFORE or AFTER the current_index passed.
    :param keep_last: Indicates
    :return:
    """

    relations = []

    # Reverse the list from current_index - 1 onwards in order to loop from end to beginning.
    if search_type == "BEFORE":
        list_to_search = reversed(dags[:-(current_index + 1)])

    # Starts from current_index + 1 onwards
    if search_type == "AFTER":
        list_to_search = dags[(len(dags) - current_index):]

    for index_dag, dag in enumerate(list_to_search):

        # Get all target tables of this DAG.
        target_tables = dag["target_tables"]

        # Check if current_reading_table matches to any target_tables of this DAG.
        if current_reading_table in target_tables:

            # Add related dag id and the affected tables.
            # relations.append({"related_dag_ids": dag["dag_id"], "affected_tables": current_reading_table})
            relations.append({"related_dag_ids": dag["dag_id"], "affected_tables": "{0:02d} - {1}".format(dag["dag_id"], current_reading_table)})

            # Check if only the first dag (last version) found should be considered.
            if keep_last:
                break

    return relations


def track_paths(dag_relations, start_dag_id=0, current_path={}, tracked_paths=[]):
    if not start_dag_id:
        start_dag_id = max(dag_relations)

    start_dag = dag_relations[start_dag_id]
    graph_node_ids = sorted(start_dag["graph_nodes_ids"], reverse=True)

    already_tracked = []
    for tracked_path in tracked_paths:
        if tracked_path[start_dag_id]:
            already_tracked.append(tracked_path[start_dag_id][1])

    for graph_node_id in graph_node_ids:
        graph_dag = dag_relations[graph_node_id]
        total_of_tasks = int(start_dag["qty_read_tasks"]) + int(graph_dag["qty_read_tasks"])
        current_path[start_dag_id] = [start_dag_id, graph_node_id, total_of_tasks]
        return track_paths(dag_relations, graph_node_id, current_path, tracked_paths)

    tracked_paths.append(current_path)
    return track_paths(dag_relations, start_dag_id=0, tracked_paths=tracked_paths)


def get_dags_relations(dags):
    """
    Returns all relations of dags between them
    :param dags: The SAFEST SORTED DAG LIST in this format [dag_id, dag_name, qty_read_tasks, target_tables, from_join_tables]
    :return: JSON Relations
    """

    dag_relations = {}

    # Start from end to beginning.
    for index_dag, dag in enumerate(reversed(dags)):

        # Get a list of all reading tables in this DAG.
        current_reading_tables = dag["from_join_tables"]

        # Instance the lists of IDs.
        before_ids = []
        before_tables_affected = []
        after_ids = []
        after_tables_affected = []
        ungrouped_before = []
        ungrouped_after = []

        # Get current DAG ID.
        current_dag_id = dag["dag_id"]

        # Create the dictionary to return.
        dag_relations[current_dag_id] = {}

        for current_reading_table in current_reading_tables:

            # For each reading table, check the LAST change it has in the DAGs PREVIOUS the current one.
            before_relations = get_reading_table_dag_relations(dags, index_dag, current_reading_table, "BEFORE")

            if before_relations:
                related_dag_ids = before_relations[0]["related_dag_ids"]
                affected_tables = before_relations[0]["affected_tables"]
                before_ids.append(related_dag_ids)
                before_tables_affected.append(affected_tables)
                #before_tables_affected.append("{0:02d} - {1}".format(related_dag_ids, affected_tables))
                ungrouped_before.append({"before_id": related_dag_ids, "before_table": affected_tables})

            # For each reading table, check the LAST change it has in the DAGs AFTER the current one.
            after_relations = get_reading_table_dag_relations(dags, index_dag, current_reading_table, "AFTER")

            if after_relations:
                related_dag_ids = after_relations[0]["related_dag_ids"]
                affected_tables = after_relations[0]["affected_tables"]
                after_ids.append(related_dag_ids)
                after_tables_affected.append(affected_tables)
                #after_tables_affected.append("{0:02d} - {1}".format(related_dag_ids, affected_tables))
                ungrouped_after.append({"after_id": related_dag_ids, "after_table": affected_tables})

        # Flat list in order to transform [[1], [2], [3, 4]] in [1, 2, 3, 4].
        before_ids = flat_list(before_ids)
        before_tables_affected = flat_list(before_tables_affected)
        after_ids = flat_list(after_ids)
        after_tables_affected = flat_list(after_tables_affected)

        # Group DAG IDs and Tables.
        before_ids = list(set(before_ids))
        before_tables_affected = list(set(before_tables_affected))
        after_ids = list(set(after_ids))
        after_tables_affected = list(set(after_tables_affected))

        dag_relations[current_dag_id]["dag_id"] = dag["dag_id"]
        dag_relations[current_dag_id]["dag_name"] = dag["dag_name"]
        dag_relations[current_dag_id]["qty_read_tasks"] = dag["qty_read_tasks"]
        dag_relations[current_dag_id]["ungrouped_before"] = ungrouped_before
        dag_relations[current_dag_id]["ungrouped_after"] = ungrouped_after
        dag_relations[current_dag_id]["before_ids"] = sorted(list(before_ids))
        dag_relations[current_dag_id]["after_ids"] = sorted(list(after_ids))
        dag_relations[current_dag_id]["before_tables_affected"] = sorted(list(before_tables_affected))
        dag_relations[current_dag_id]["after_tables_affected"] = sorted(list(after_tables_affected))
        dag_relations[current_dag_id]["total_before_relations"] = len(before_ids)
        dag_relations[current_dag_id]["total_after_relations"] = len(after_ids)
        dag_relations[current_dag_id]["total_relations"] = len(before_ids) + len(after_ids)
        dag_relations[current_dag_id]["graph_nodes_ids"] = list(dag_relations[current_dag_id]["before_ids"]).copy()
        dag_relations[current_dag_id]["graph_nodes_tables"] = list(dag_relations[current_dag_id]["before_tables_affected"]).copy()

    # Add current dag id, to every "after_id" of DAG
    for dag_index, dag_relation in enumerate(dag_relations):
        ungrouped_after_items = dag_relations[dag_relation]["ungrouped_after"]
        for ungrouped_after_item in ungrouped_after_items:
            # Assign "after_id" of the current dag loop.
            this_after_id = ungrouped_after_item["after_id"]

            # The initial "graph_nodes_ids" and "graph_nodes_tables" are exactly the same as "before_ids" and
            # "before_tables_affected". Posteriorly, they are incremented with this dag id and table for every
            # after_id existent in this dag.
            graph_nodes_ids = list(dag_relations[this_after_id]["graph_nodes_ids"]).copy()
            graph_nodes_tables = list(dag_relations[this_after_id]["graph_nodes_tables"]).copy()

            # Increment with dag id and table for this after_id.
            graph_nodes_ids.append(dag_relations[dag_relation]["dag_id"])
            graph_nodes_tables.append(dag_relations[dag_relation]["before_tables_affected"])

            dag_relations[this_after_id]["graph_nodes_ids"] = sorted(list(set(graph_nodes_ids)))
            dag_relations[this_after_id]["graph_nodes_tables"] = sorted(list(set(flat_list(graph_nodes_tables))))

    return dag_relations


# Read and Prepare DAGs Files
def convert_file_to_list(file_path):
    """
    Return all the file content as a list.
    :param file_path: File path to retrieve.
    :return: List of content split by line.
    """

    # Read file content to variable.
    file_content = open(file_path).read()

    # Split content in lines.
    file_lines = file_content.splitlines()

    return file_lines


def clear_dag_value(raw_value, type_value):
    """
    Clean DAG value specification removing specific characters and/or symbols.
    :param raw_value: Raw value with all characters and symbols.
    :param type_value: "task_id" or "bash_command" - Define what type of value should be cleaned.
    :return: Clear value after removing parts of the "raw_value"
    """

    if type_value == "dag_name":
        clear_value = raw_value.strip().replace("task_id=", "").replace("'", "").strip()
        clear_value = clear_value[:clear_value.rfind("_")]

    if type_value == "task_id":
        clear_value = raw_value.strip().replace("task_id=", "").replace("'", "").strip()

    if type_value == "sql":
        clear_value = raw_value.strip().replace("sql=", "").replace("'", "").replace(",", "").strip()

    return clear_value


def find_dags(file_path):
    """
    Find all DAGs specifications existent in the file.
    :param file_path: File path to retrieve.
    :return: All the DAGs specification existent in the file.
    """
    file_list = convert_file_to_list(file_path)
    dags_specs = []
    dag_id = 0
    dag_name = ""

    # Scans all lines of file looking for DAG specification.
    for line, file_line in enumerate(file_list):

        # For every "task_id" found, "bash_command" must appear right next line.
        if "task_id=" in file_line:

            # Get "dag_id" value. It will be incremented every time "dag_name" changes.
            if dag_name != clear_dag_value(file_line, "dag_name"):
                dag_id += 1

            # Get "dag_name" value. It will be part of "task_id".
            dag_name = clear_dag_value(file_line, "dag_name")

            # Get "task_id" value.
            task = clear_dag_value(file_line, "task_id")

            # Then get the respective "bash_command" on the next line.
            path = clear_dag_value(file_list[line + 1], "sql")

            # Add DAG specification to a list.
            dags_specs.append({"dag_id": dag_id, "dag_name": dag_name, "task": task, "path": path})

    return dags_specs


def get_sql_file_content(sql_file_path):
    """
    Return file content.
    :param sql_file_path: File path like /scripts/controle_carga/controle_carga_c.sql
    :return: The content in this file.
    """

    # Set query initially as empty.
    file_content = ""

    # Adjust file path.
    sql_file_path = os.path.normpath(os.getcwd() + "/" + sql_file_path)

    # Ensure this file exists.
    if os.path.exists(sql_file_path):
        with open(sql_file_path, encoding="utf-8") as sql_file:
            for line in sql_file:
                file_content += line
    else:
        # Increment FILES_NOT_FOUND global variable.
        FILES_NOT_FOUND.append(sql_file_path)

    return file_content


def group_values_by_dag(flat_dags_tables_list):
    """
    Group DAGs values by it's ID.
    :param flat_dags_tables_list: Flat DAGs list like this {dag_id, dag_name, target_table, from_tables, task_id}
    :return: DAGs values grouping all "target_table", "from_join_tables" and adding "qtd_task"
    """

    # Sort lisst by "dag_id"
    flat_dags_tables_list.sort(key=lambda content: content["dag_id"])

    # Group list by "dag_id"
    dag_groups = groupby(flat_dags_tables_list, lambda content: content['dag_id'])

    grouped_dags = []
    for dag_name, group in dag_groups:

        # Create an empty dictionary to store DAG tables.
        target_tables = []
        from_join_tables = []

        # Start "qty_read_tasks" as 0.
        qty_read_tasks = 0

        for content in group:
            dag_id = content["dag_id"]
            dag_name = content["dag_name"]
            qty_read_tasks += 1
            target_tables += content["target_table"]
            from_join_tables += content["from_join_tables"]

        grouped_dags.append({"dag_id": dag_id,
                             "dag_name": dag_name,
                             "qty_read_tasks": qty_read_tasks,
                             "target_tables": sorted(list(set(target_tables))),
                             "from_join_tables": sorted(list(set(from_join_tables)))
                             })

    return grouped_dags


# Plot Graph
def plot_dependencies_graph(dags_relations, output_dir, file_name):
    """
    Generate a left-to-right hierarchical graph representing the DAGs flow.
    :param dags_relations:
    :param file_name: File name WITHOUT it's extension that will be generated in the "output_dir"
    :param output_dir: Directory to generate the graph.
    :return: Create a PDF.
    """

    # Set file_format
    file_format = "pdf"

    # Start a GraphViz using "dot" engine.
    dags_graph = Digraph("G", engine="dot", filename=file_name, format=file_format)

    # Set to visually display nodes hierarchy from left to right.
    dags_graph.attr(rankdir="LR")

    # Set nodes arrangement to control graph plotting size.
    dags_graph.attr(ranksep="2", nodesep="0.25")

    html_content = """
                    <
                    <table border="0" cellborder="0" cellspacing="1" fixedsize="False">
                        <tr><td align="center" width="130"><font point-size="6"><i>{label_dag_name}</i></font></td></tr>
                        <tr><td align="center"><font point-size="25"><b>{label_dag_id}</b></font></td></tr>
                        <tr><td align="left"></td></tr>
                        <tr><td align="left">Nº DE TASKS <b>{label_qty_read_tasks}</b></td></tr>
                        <tr><td align="center"><b>DEPENDE DE</b></td></tr>
                        <tr><td align="left">{label_nodes_ids}</td></tr>
                        <tr><td align="left"><font point-size="6">{label_nodes_tables}</font></td></tr>
                    </table>
                    >
                    """

    html_content = """
                    <
                    <table border="0" cellborder="0" cellspacing="1" fixedsize="False">
                        <tr><td align="center" width="130"><font point-size="6"><i>{label_dag_name}</i></font></td></tr>
                        <tr><td align="center"><font point-size="25"><b>{label_dag_id}</b></font></td></tr>
                        <tr><td align="left"></td></tr>
                        <tr><td align="center"><b>DEPENDENCIES</b></td></tr>
                        <tr><td align="left">{label_nodes_ids}</td></tr>
                        <tr><td align="left"><font point-size="6">{label_nodes_tables}</font></td></tr>
                    </table>
                    >
                    """

    for dag_index, dag in enumerate(dags_relations):

        # Prepare the labels that will be printed inside the boxes.
        label_dag_id = str(dag["dag_id"])
        label_dag_name = str(dag["dag_name"])
        label_qty_read_tasks = str(dag["qty_read_tasks"])
        label_nodes_ids = dag["graph_nodes_ids"] if dag["graph_nodes_ids"] else "-"
        label_nodes_tables = "<BR ALIGN='LEFT'/>".join(dag["graph_nodes_tables"]) + "<BR ALIGN='LEFT'/>" if dag[
            "graph_nodes_tables"] else " "

        # Set ids to be used in the edges generation.
        dag_id = dag["dag_id"]
        graphs_ids = dag["graph_nodes_ids"]

        # Set color based on the number of dependencies
        box_color = ""
        total_property = "graph_nodes_ids"
        if len(dag[total_property]) == 0:
            box_color = "#00C851"  # Verde
        elif 0 < len(dag[total_property]) <= 1:
            box_color = "#33b5e5"  # Azul
        elif 1 < len(dag[total_property]) <= 2:
            box_color = "#ffbb33"  # Amarelo
        else:
            box_color = "#ff4444"  # Vermelho

        dags_relations[dag_index]["color"] = box_color

        # Define node box colors based on the generated bins.
        fill_color = dag["color"]
        font_color = "white"

        # Create a filled color node containing DAGs properties.
        dags_graph.node(label_dag_id,
                        label=html_content.strip().format(label_dag_name=label_dag_name,
                                                          label_dag_id=label_dag_id,
                                                          label_qty_read_tasks=label_qty_read_tasks,
                                                          label_nodes_ids=label_nodes_ids,
                                                          label_nodes_tables=label_nodes_tables),
                        shape="Mrecord",
                        fontcolor=font_color,
                        style="filled",
                        fontname="Verdana",
                        fontsize="8",
                        fillcolor=fill_color,
                        color="#C0C0C0")

        for graphs_id in graphs_ids:
            dags_graph.edge(str(graphs_id), str(dag_id), style="solid", color="#A9A9A9", label="")

    # Generate the graph file.
    dags_graph.render(directory=output_dir, view=True)
    print("GRAPH successfully generated in {0}{1}.{2}".format(output_dir, file_name, file_format))


# Main functions
def get_grouped_tables_by_dag(dag_group_file_path):
    # Read all DAGs existent in "dag_group_file_path"
    dags_list = find_dags(dag_group_file_path)

    # Get the dag's group name.
    dag_group_name = os.path.basename(dag_group_file_path)

    # DAGs target and from tables list.
    dags_tables_list = []

    # Loop all DAGs info.
    for dag in dags_list:
        raw_sql = get_sql_file_content(dag["path"])
        dag_id = dag["dag_id"]
        dag_name = dag["dag_name"]
        task_id = dag["task"].replace("'", "").replace(",", "")

        # Append the target and from tables used in this DAG.
        dags_tables_list.append(get_sql_details(raw_sql, dag_id, dag_name, task_id))

    # Flat the list putting the key (task_id) as a dictionary element.
    flat_dags_tables_list = []
    for dag_tables in dags_tables_list:
        task_id = list(dag_tables.keys())[0]
        dag_tables_values = list(dag_tables.values())[0]
        dag_tables_values["task_id"] = task_id
        flat_dags_tables_list.append(dag_tables_values)

    # Group the list by DAG id
    grouped_dags_list = group_values_by_dag(flat_dags_tables_list)

    return grouped_dags_list


def create_relation_csv(dag_group_file_path, output_dir, csv_file_name):
    """
    Create CSV file containing all grouped DAGs.
    :param dag_group_file_path: File path of ".py" that contains all DAGs and TASKS.
    :param output_dir: The output directory do save the CSV file.
    :param csv_file_name: The CSV file name without extension.
    :return:
    """
    # Get grouped dags list.
    grouped_dags_list = get_grouped_tables_by_dag(dag_group_file_path)

    # Set file format.
    file_format = "csv"

    # Convert the list to JSON.
    grouped_dags_json = json.dumps(grouped_dags_list)

    # Convert a JSON string to pandas object.
    grouped_dags_df = pd.read_json(grouped_dags_json)

    # Sort by "dag_id"
    grouped_dags_df.sort_values(by=["dag_id"])

    # Create CSV file.
    grouped_dags_df.to_csv("{0}{1}.{2}".format(output_dir, csv_file_name, file_format))
    print("CSV successfully generated in {0}{1}.{2}".format(output_dir, csv_file_name, file_format))


def create_relation_graph(dag_group_file_path, output_dir, pdf_file_name):
    """
    Create GRAPH file containing all grouped DAGs.
    :param dag_group_file_path: File path of ".py" that contains all DAGs and TASKS.
    :param output_dir: The output directory do save the PDF file.
    :param pdf_file_name: The PDF file name without extension.
    :return:
    """
    grouped_tables_by_dag = get_grouped_tables_by_dag(dag_group_file_path)
    dags_relations = get_dags_relations(grouped_tables_by_dag)
    dags_relations = list(dags_relations.values())

    plot_dependencies_graph(dags_relations, output_dir, pdf_file_name)


def view_relation_json(dag_group_file_path):
    """
    View JSON containing all grouped DAGs.
    :param dag_group_file_path: File path of ".py" that contains all DAGs and TASKS.
    :return: JSON
    """
    # Get grouped dags list.
    grouped_dags_list = get_grouped_tables_by_dag(dag_group_file_path)

    # Print JSON relation.
    return grouped_dags_list


def main():

    dag_group_file_path = CURRENT_DIR + "//dag-sample.py"
    create_relation_graph(dag_group_file_path, "output", "optimised-dag")

    # """ Main Program """
    # # Parse Cmdline Arguments
    # parser = argparse.ArgumentParser()
    # parser.add_argument("-type", "--execution_type", default="graph", type=str, required=True,
    #                     help="Type of execution 'graph', 'csv', 'json'.")
    # parser.add_argument("-dags", "--dags_file_path", default="", type=str, required=True,
    #                     help="The .py file in which contains all DAGs and TASKs described.")
    # parser.add_argument("-dir", "--output_dir", default=CURRENT_DIR, type=str, required=False,
    #                     help="Output directory to save any file created by the script.")
    # parser.add_argument("-file", "--filename", default=CURRENT_DIR, type=str, required=False,
    #                     help="The file name without extension that will be used by the script.")
    #
    # args = parser.parse_args()
    #
    # if args.execution_type == "graph":
    #     create_relation_graph(args.dags_file_path, args.output_dir, args.filename)
    # elif args.execution_type == "csv":
    #     create_relation_csv(args.dags_file_path, args.output_dir, args.filename)
    # elif args.execution_type == "json":
    #     view_relation_json(args.dags_file_path)
    # else:
    #     print("Invalid execution type. Use -type ['graph', 'csv', 'json']")


if __name__ == '__main__':
    main()


