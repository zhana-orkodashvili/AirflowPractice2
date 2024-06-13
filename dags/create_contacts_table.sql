
CREATE TABLE IF NOT EXISTS contacts (
    id SERIAL PRIMARY KEY,
    name text,
    surname text,
    phone text
);

CREATE OR REPLACE FUNCTION insert_test_data()
RETURNS void
LANGUAGE plpgsql
AS
$$
BEGIN
    INSERT INTO contacts (name, surname, phone)
    VALUES
    ('Zhana', 'Orkodashvili', '995123456789'),
    ('John', 'Doe', '995987654321');
END;
$$;
