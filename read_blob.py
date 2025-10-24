import os
import psycopg2
from config import load_config


def read_blob(part_id, output_dir):
    """Read a BLOB from the database and save it to a file."""
    config = load_config()

    try:
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT part_name, file_extension, drawing_data
                    FROM part_drawings
                    INNER JOIN parts ON parts.part_id = part_drawings.part_id
                    WHERE parts.part_id = %s
                """, (part_id,))

                row = cur.fetchone()

                if not row:
                    print(f"No record found for part_id={part_id}")
                    return

                part_name, file_extension, drawing_data = row

                # Ensure the output directory exists
                os.makedirs(output_dir, exist_ok=True)

                # Build file path safely
                file_path = os.path.join(output_dir, f"{part_name}.{file_extension}")

                with open(file_path, 'wb') as f:
                    f.write(drawing_data)

                print(f"Saved {file_path}")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Database error: {error}")


if __name__ == '__main__':
    read_blob(1, 'output/')
    read_blob(2, 'output/')
