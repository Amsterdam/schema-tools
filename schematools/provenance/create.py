from schematools.types import DatasetSchema


class ProvenaceIteration:
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

    def set_dataset_for_final_listing(self, dataschema: DatasetSchema):
        """Setting dataset level to add later als wrapper"""

        if type(dataschema) is dict:

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

    def get_provenance_per_table(self, dictionary):
        """Calling the processing to extract the provenance data per table"""

        # loop trough all existing tables and proces data for provenance
        # on each and add them each to the final_dict outcome
        for n in range(0, self.number_of_tables):

            self.temp_dic_tables["table"] = dictionary[n]["id"]
            self.temp_dic_tables["provenance"] = (
                dictionary[n]["provenance"]
                if "provenance" in dictionary[n]
                else "na"  # not applicable
            )
            self.temp_dic_tables["properties"] = []
            self.get_table_columns(dictionary[n]["schema"]["properties"])
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

        # Setup list for holding column names to check if a column is already added
        column_list = []
        self.temp_dic_columns = {}
        for column in dictionary:

            # Check if column has provenance specification,
            # if so, add the value of this specification in the dictionary
            for column_property, value in dictionary[column].items():

                if column_property == "provenance":
                    column_list.append(column)
                    self.temp_dic_columns[column] = value

            # Add the columns to the dictionary that have no provenance
            # and exclude schema as it will never be added in the database
            if column not in column_list and column != "schema":
                self.temp_dic_columns[column] = column
