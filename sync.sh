#!/bin/bash

QUESTDB_URL=http://35.203.150.125:9000
PG_URL=postgres://public_readonly:nearprotocol@104.199.89.51/mainnet_explorer

ROW_LIMIT=1000
# TODO: Update query so that rows at the limit edge don't get dropped (because of timestamp filtering)

LATEST=$(curl -G \
  --data-urlencode "query=select cast(max(ts) as long) from 1.csv" \
  "$QUESTDB_URL/exec" | jq '.dataset[0][0] // 0' )
echo "Latest: $LATEST"

# | curl -F data=@- /imp << EOF
time psql $PG_URL --csv > 1.csv << EOF

select * from ( 
	select
		to_char(to_timestamp(receipt_included_in_block_timestamp / 1000000000), 'yyyy-MM-dd"T"HH24:MI:SS.US"Z"') as ts,
		receipt_id,
		index_in_action_receipt,
		action_kind,
		args->>'deposit' as deposit,
		args->>'method_name' as method_name,
		args->>'args_json' as args_json,
		args->'args_json'->>'receiver_id' as args_receiver_id,
		receipt_receiver_account_id,
		receipt_predecessor_account_id,
		transaction_hash,
		signer_account_id,
		signer_public_key
	from action_receipt_actions
	join receipts using (receipt_id)
	join transactions on originated_from_transaction_hash = transaction_hash
    where receipt_included_in_block_timestamp > $LATEST * 1000
	order by receipt_included_in_block_timestamp
	limit $ROW_LIMIT
) filtered
order by ts, index_in_action_receipt

EOF

time curl -F data=@1.csv "$QUESTDB_URL/imp"

