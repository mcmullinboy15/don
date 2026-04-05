CREATE TABLE users (
    id UUID NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    email STRING NOT NULL UNIQUE,
    name STRING NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- comment added
