from schematools.types import DatasetSchema


class ProvenaceIteration:
    """Extract provenance elements in DatasetSchema (json) so it can be used for automatic processing"""

    final_dic = {}
    temp_dic_tables = {}
    temp_dic_columns = {}
    number_of_tables = 0

    def __init__(self, dataschema: DatasetSchema):
        """Trigger processing"""

        self.get_number_of_tables(dataschema)
        self.set_dataset_for_final_listing(dataschema)

    def get_number_of_tables(self, dataschema: DatasetSchema):
        """Retrieving the number of tables in datasetschema for looping purpose"""
        for item in dataschema:
            if item == "tables":
                self.number_of_tables = len(dataschema[item])
                return self.number_of_tables

    def set_dataset_for_final_listing(self, dataschema: DatasetSchema):
        """Setting dataset level to add later als wrapper"""

        if type(dataschema) is dict:

            # At first make root branch dataset and take id field as value for dataschema attribute
            self.final_dic["dataset"] = dataschema["id"]
            # Add provenance element on dataset level
            self.final_dic["provenance"] = (
                dataschema["provenance"]
                if "provenance" in dataschema
                else "na"  # not applicable
            )
            # and make tables branch to hold the tables provenance data
            self.final_dic["tables"] = []

            for item in dataschema:

                self.get_provenance_per_table(dataschema[item])

    def get_provenance_per_table(self, dictionary):
        """Calling the processing to extract the provenance data per table"""

        if type(dictionary) is list:
            # loop trough all existing tables and proces data for provenance on each and add them each to the final_dict outcome
            for n in range(0, self.number_of_tables):

                self.temp_dic_tables["table"] = dictionary[n]["id"]
                self.temp_dic_tables["provenance"] = (
                    dictionary["provenance"]
                    if "provenance" in dictionary
                    else "na"  # not applicable
                )
                self.temp_dic_tables["properties"] = []
                self.get_provenance(dictionary[n], n)
                # add table result within element tables in final result
                self.final_dic["tables"].append(self.temp_dic_tables)
                # clean up temp_dic to enforce a new object for next table
                self.temp_dic_tables = {}

    def get_provenance(self, dictionary, parent_item):
        """Extrating the provenance data within a table using recursive call backs (calling it self)"""
        if type(dictionary) is dict:
            for i in dictionary:
                self.temp_dic_columns = {}

                if i == "provenance":

                    self.temp_dic_columns[dictionary[i]] = parent_item
                    self.temp_dic_tables["properties"].append(
                        dict(self.temp_dic_columns)
                    )

                else:
                    # contiue
                    self.get_provenance(dictionary[i], i)
