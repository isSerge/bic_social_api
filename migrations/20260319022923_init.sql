CREATE TABLE IF NOT EXISTS likes (
	user_id UUID NOT NULL,
	content_type TEXT NOT NULL,
	content_id UUID NOT NULL,
	created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	PRIMARY KEY (user_id, content_type, content_id)
);

CREATE TABLE IF NOT EXISTS like_counts (
	content_type TEXT NOT NULL,
	content_id UUID NOT NULL,
	count BIGINT NOT NULL DEFAULT 0 CHECK (count >= 0),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	PRIMARY KEY (content_type, content_id)
);

CREATE INDEX IF NOT EXISTS idx_likes_user_created_content
	ON likes (user_id, created_at DESC, content_id DESC);

CREATE INDEX IF NOT EXISTS idx_likes_content_created
	ON likes (content_type, content_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_likes_created_at
	ON likes (created_at DESC);
