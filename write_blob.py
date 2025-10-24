import psycopg2
from config import load_config


def write_blob(part_id, path_to_file, file_extension):
    """Insert or update a BLOB in the part_drawings table."""
    params = load_config()

    # Read the binary file safely
    with open(path_to_file, 'rb') as f:
        data = f.read()

    try:
        with psycopg2.connect(**params) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO parts(part_id, part_name)
                    VALUES (%s, %s)
                    ON CONFLICT (part_id) DO NOTHING
                """, (part_id, f"AutoPart_{part_id}"))

                # UPSERT into part_drawings
                cur.execute("""
                    INSERT INTO part_drawings(part_id, file_extension, drawing_data)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (part_id)
                    DO UPDATE SET
                        file_extension = EXCLUDED.file_extension,
                        drawing_data = EXCLUDED.drawing_data
                """, (part_id, file_extension, psycopg2.Binary(data)))

            conn.commit()
            print(f"Image for part_id={part_id} inserted or updated successfully.")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Database error: {error}")


if __name__ == '__main__':
    write_blob(1, 'cat1.jpg', 'jpg')
    write_blob(2, 'cat2.jpg', 'jpg')
