from google.cloud import spanner
from google.api_core.exceptions import GoogleAPICallError
import datetime
from google.oauth2.service_account import Credentials


def write_to(database):
    record = [[
        1041613562310836275,
        'test_name'
    ]]

    columns = ("id", "name")

    insert_errors = []

    try:
        with database.batch() as batch:
            batch.insert_or_update(
                table="guild",
                columns=columns,
                values=record,
            )
    except GoogleAPICallError as e:
        print(f'error: {e}')
        insert_errors.append(e.message)
        pass

    return insert_errors

if __name__ == "__main__":
    credentials = Credentials.from_service_account_file(r'path\to\a.json')
    instance_id = 'instance-name'
    database_id = 'database-name'
    spanner_client = spanner.Client(project='project-name', credentials=credentials)
    print(f'spanner creds: {spanner_client.credentials}')
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)
    insert_errors = write_to(database)