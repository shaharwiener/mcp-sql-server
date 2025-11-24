import json
import pyodbc


class ResponseService:
    
    def process_json_response_list(self, response_row):
        if response_row in ('There is nothing', 'Update Successful'):
            return [response_row]  # Wrap single response messages in a list
        elif isinstance(response_row, list):
            # Process each row in the list if it's already a list of rows
            return self.rows_to_dicts(response_row)
        else:
            # Wrap a single row or value in a list after converting it to dict
            return [self.row_to_dict(response_row)]

    def process_json_response(self, response_row):
        if response_row in ('There is nothing', 'Update Successful'):
            return response_row

        if isinstance(response_row, list):
            return self.row_to_dict(response_row[0])
        try:

            return self.row_to_dict(response_row)
        except Exception as e:
            return json.dumps({'error': str(e)})

    def row_to_dict(self, row):
        """Convert a single pyodbc.Row to a dictionary, or handle a single value."""
        if isinstance(row, pyodbc.Row):
            # It's a pyodbc.Row object
            return dict(zip([column[0] for column in row.cursor_description], row))
        else:
            # It's a single value (like an int)
            return {"value": row}

    def rows_to_dicts(self, rows):
        """Convert a list of pyodbc.Row objects to a list of dictionaries."""
        return [self.row_to_dict(row) for row in rows]
