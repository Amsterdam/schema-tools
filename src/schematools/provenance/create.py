from __future__ import annotations

from schematools.types import DatasetSchema


class ProvenanceIteration:
    """Extract provenance elements in DatasetSchema (json)
    so it can be used for automatic processing"""

    final_dic = {}
    final_dic_all_columns = {}
    temp_dic_tables = {}
    temp_dic_columns = {}
    number_of_tables = 0

    def __init__(self, dataschema: DatasetSchema):
        """Trigger processing"""

        self.set_number_of_tables(dataschema)
        self.set_dataset_for_final_listing(dataschema)

    def set_number_of_tables(self, dataschema: DatasetSchema):
        """Retrieving the number of tables in datasetschema for looping purpose"""
        for item in dataschema:
            if item == "tables":
                self.number_of_tables = len(dataschema[item])
                return self.number_of_tables
        return None

    def set_dataset_for_final_listing(self, dataschema: DatasetSchema):
        """Setting dataset level to add later als wrapper"""

        if isinstance(dataschema, dict):

            # At first make root branch dataset
            # and take id field as value for dataschema attribute
            self.final_dic["dataset"] = dataschema["id"]
            # Add provenance element on dataset level
            self.final_dic["provenance"] = dataschema.get("provenance", "na")
            # and make tables branch to hold the tables provenance data
            self.final_dic["tables"] = []

            for item in dataschema:
                if item == "tables":
                    self.get_provenance_per_table(dataschema[item])

    def get_provenance_per_table(self, dicts):
        """Calling the processing to extract the provenance data per table"""

        # loop trough all existing tables and proces data for provenance
        # on each and add them each to the final_dict outcome
        for d in dicts[: self.number_of_tables]:
            self.temp_dic_tables["table"] = d["id"]
            self.temp_dic_tables["provenance"] = d.get("provenance", "na")
            self.temp_dic_tables["properties"] = []
            self.get_table_columns(d["schema"]["properties"])
            # add table columns (within element properties) to table
            self.temp_dic_tables["properties"].append(dict(self.temp_dic_columns))
            # add table result (within element tables) in final result
            self.final_dic["tables"].append(self.temp_dic_tables)
            # clean up temp_dic* to enforce a new object for next table
            self.temp_dic_tables = {}
            self.temp_dic_columns = {}

    def get_table_columns(self, dictionary):
        """Extrating the columns and if provenance then adding its value,
        resulting in a dictionary of source name : column name"""

        done = set()
        self.temp_dic_columns = {}
        for column in dictionary:
            if "provenance" in dictionary[column]:
                done.add(column)
                self.temp_dic_columns[column] = dictionary[column]["provenance"]

            # Add the columns to the dictionary that have no provenance
            # and exclude schema as it will never be added in the database
            if column not in done and column != "schema":
                self.temp_dic_columns[column] = column
