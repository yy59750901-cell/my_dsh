ALTER TABLE session_events ADD COLUMN surface_op TEXT NULL CHECK (surface_op IS NULL OR json_valid(surface_op));
