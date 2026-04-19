CREATE TABLE IF NOT EXISTS chats (
    chat_id BIGINT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS links (
    id              BIGSERIAL PRIMARY KEY,
    url             TEXT NOT NULL UNIQUE,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_checked_at TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01 00:00:00+00'
);

CREATE TABLE IF NOT EXISTS subscriptions (
    chat_id    BIGINT NOT NULL REFERENCES chats(chat_id) ON DELETE CASCADE,
    link_id    BIGINT NOT NULL REFERENCES links(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (chat_id, link_id)
);

CREATE TABLE IF NOT EXISTS link_tags (
    id      BIGSERIAL PRIMARY KEY,
    chat_id BIGINT NOT NULL,
    link_id BIGINT NOT NULL,
    tag     TEXT NOT NULL,
    FOREIGN KEY (chat_id, link_id) REFERENCES subscriptions(chat_id, link_id) ON DELETE CASCADE,
    UNIQUE(chat_id, link_id, tag)
);

CREATE TABLE IF NOT EXISTS users (
    id         BIGINT PRIMARY KEY,
    username   TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_subscriptions_link_id ON subscriptions(link_id);
CREATE INDEX IF NOT EXISTS idx_links_last_checked_at ON links(last_checked_at);
