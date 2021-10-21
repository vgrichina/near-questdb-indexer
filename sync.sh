#!/bin/bash
set -ex

QUESTDB_URL=http://35.203.150.125:9000
PG_URL=${PG_URL:=postgres://public_readonly:nearprotocol@104.199.89.51/mainnet_explorer}

# echo $PG_URL

# NOTE: This specifies number of blocks. Might need to be tuned to avoid inefficient join by Postgres.
ROW_LIMIT=3000

LATEST_JSON=$(curl -G \
  --data-urlencode "query=select block_timestamp from actions order by ts desc limit 1" \
  "$QUESTDB_URL/exec")

LATEST_TIMESTAMP=$(echo $LATEST_JSON | jq -r '.dataset[0][0] // 0')

echo "Latest: $LATEST $LATEST_TIMESTAMP"

# | curl -F data=@- /imp << EOF
# cat << EOF
time psql $PG_URL << EOF

CREATE OR REPLACE FUNCTION pg_temp.test(block_limit numeric, last_timestamp numeric)
	RETURNS table(
		ts text,
		block_timestamp numeric,
		receipt_id text,
		index_in_action_receipt integer,
		action_kind text,
		deposit numeric,
		method_name text,
		args_json text,
		args_receiver_id text,
		receiver_account_id text,
		predecessor_account_id text,
		transaction_hash text,
		index_in_chunk integer,
		signer_account_id text,
		signer_public_key text
	) AS
\$func\$
DECLARE
	elem text;
BEGIN
	FOR elem IN
		select block_hash from blocks
		where blocks.block_timestamp > last_timestamp
			-- and exists(select 1 from transactions where transactions.included_in_block_hash = block_hash)
		order by blocks.block_timestamp
		limit block_limit
	LOOP
		RETURN query select * from (
			select
				to_char(to_timestamp(transactions.block_timestamp / 1000000000), 'yyyy-MM-dd"T"HH24:MI:SS.US"Z"') as ts,
				transactions.block_timestamp as block_timestamp,
				receipts.receipt_id,
				action_receipt_actions.index_in_action_receipt,
				action_receipt_actions.action_kind::text,
				(args->>'deposit')::numeric / 1e24 as deposit,
				args->>'method_name' as method_name,
				args->>'args_json' as args_json,
				args->'args_json'->>'receiver_id' as args_receiver_id,
				receipts.receiver_account_id,
				receipts.predecessor_account_id,
				transactions.transaction_hash,
				transactions.index_in_chunk,
				transactions.signer_account_id,
				transactions.signer_public_key
			from transactions
			join receipts on originated_from_transaction_hash = transactions.transaction_hash
			join action_receipt_actions using (receipt_id)
			where transactions.included_in_block_hash = elem
		) filtered
		order by ts, index_in_action_receipt;
   END LOOP;
END
\$func\$ LANGUAGE plpgsql;

\\copy (select * from pg_temp.test($ROW_LIMIT, $LATEST_TIMESTAMP)) to 'tmp.csv' csv header;

EOF

time curl -F schema=@schema.json -F data=@tmp.csv "$QUESTDB_URL/imp?name=actions&timestamp=ts&fmt=tabular"

