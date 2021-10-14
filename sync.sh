#!/bin/bash

QUESTDB_URL=http://35.203.150.125:9000
PG_URL=${PG_URL:=postgres://public_readonly:nearprotocol@104.199.89.51/mainnet_explorer}

set -ex
# echo $PG_URL

# NOTE: This specifies number of blocks. Might need to be tuned to avoid inefficient join by Postgres (usually kicks in at around 75 limit).
ROW_LIMIT=50

LATEST_JSON=$(curl -G \
  --data-urlencode "query=select block_timestamp from actions order by ts desc limit 1" \
  "$QUESTDB_URL/exec")

LATEST_TIMESTAMP=$(echo $LATEST_JSON | jq '.dataset[0][0] // 0')

echo "Latest: $LATEST $LATEST_TIMESTAMP"

# | curl -F data=@- /imp << EOF
# cat << EOF
time psql $PG_URL --csv > tmp.csv << EOF

select * from (
	select
		to_char(to_timestamp(transactions.block_timestamp / 1000000000), 'yyyy-MM-dd"T"HH24:MI:SS.US"Z"') as ts,
		transactions.block_timestamp as block_timestamp,
		receipt_id,
		index_in_action_receipt,
		action_kind,
		args->>'deposit' as deposit,
		args->>'method_name' as method_name,
		args->>'args_json' as args_json,
		args->'args_json'->>'receiver_id' as args_receiver_id,
		receipts.receiver_account_id,
		receipts.predecessor_account_id,
		transaction_hash,
		transactions.index_in_chunk,
		signer_account_id,
		signer_public_key
	from (
		select * from blocks
		where blocks.block_timestamp > $LATEST_TIMESTAMP * 1000
		order by blocks.block_timestamp
		limit $ROW_LIMIT
	) as blocks
	join transactions on included_in_block_hash = block_hash
	join receipts on originated_from_transaction_hash = transaction_hash
	join action_receipt_actions using (receipt_id)
) filtered
order by ts, index_in_action_receipt

EOF

time curl -F schema=@schema.json -F data=@tmp.csv "$QUESTDB_URL/imp?name=actions&timestamp=ts&fmt=tabular"

