CREATE TABLE IF NOT EXISTS chats (
    chat_id BIGINT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS links (
    id         BIGSERIAL PRIMARY KEY,
    url        TEXT NOT NULL,
    chat_id    BIGINT NOT NULL REFERENCES chats(chat_id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(url, chat_id)
);

CREATE TABLE IF NOT EXISTS link_tags (
    id      BIGSERIAL PRIMARY KEY,
    link_id BIGINT NOT NULL REFERENCES links(id) ON DELETE CASCADE,
    tag     TEXT NOT NULL,
    UNIQUE(link_id, tag)
);

CREATE TABLE IF NOT EXISTS users (
    id         BIGINT PRIMARY KEY,
    username   TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_links_chat_id ON links(chat_id);
CREATE INDEX IF NOT EXISTS idx_links_url ON links(url);
CREATE INDEX IF NOT EXISTS idx_link_tags_link_id ON link_tags(link_id);
