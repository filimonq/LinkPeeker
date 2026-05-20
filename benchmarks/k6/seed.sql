-- docker compose exec -T postgres psql -U postgres -d linktracker < benchmarks/k6/seed.sql

INSERT INTO chats (chat_id)
SELECT generate_series(1, 1000)
ON CONFLICT DO NOTHING;

INSERT INTO links (url, updated_at, last_checked_at)
SELECT
    'https://github.com/load-test/repo-' || n || '/issues/' || m,
    NOW() - (random() * interval '30 days'),
    NOW() - (random() * interval '7 days')
FROM generate_series(1, 1000) n,
     generate_series(1, 100)  m
ON CONFLICT (url) DO NOTHING;

INSERT INTO subscriptions (chat_id, link_id)
SELECT
    n,
    (SELECT id FROM links
     WHERE url = 'https://github.com/load-test/repo-' || n || '/issues/' || m)
FROM generate_series(1, 1000) n,
     generate_series(1, 100)  m
ON CONFLICT DO NOTHING;

SELECT
    (SELECT COUNT(*) FROM chats)         AS chats,
    (SELECT COUNT(*) FROM links)         AS links,
    (SELECT COUNT(*) FROM subscriptions) AS subscriptions;