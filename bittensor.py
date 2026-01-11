import bittensor as bt
wallet = bt.Wallet(name = 'ismarty_coldkey', hotkey = 'ismarty_hotkey' )
wallet.create_if_non_existent()