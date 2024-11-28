from google.cloud import bigtable


def check_bigtable_object_exists(client, project_id, instance_id, table_id, row_key):
    instance = client.instance(instance_id)
    table = instance.table(table_id)
    row = table.read_row(row_key.encode())
    return row is not None
