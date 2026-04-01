import unittest
from data_migrator.mapping import json_user_to_row, row_to_json

class MappingTest(unittest.TestCase):
    def test_roundtrip(self):
        discord_key = "123456789"
        state = {
            "uid": "800123456",
            "hsr_uid": "700806861",
            "enabled": True,
            "notified_full": False,
            "ltuid_v2": "cipher_ltuid",
            "ltoken_v2": "cipher_ltoken",
            "daily_spent": 67,
            "last_resin": 140,
        }
        
        row = json_user_to_row(discord_key, state)
        self.assertEqual(row["discord_id"], 123456789)
        self.assertEqual(row["genshin_uid"], 800123456)
        self.assertEqual(row["hsr_uid"], 700806861)
        self.assertEqual(row["enabled"], True)
        self.assertEqual(row["daily_spent"], 67)
        self.assertEqual(row["last_resin"], 140)

        j = row_to_json(row)
        self.assertEqual(j["uid"], "800123456")
        self.assertEqual(j["hsr_uid"], "700806861")
        self.assertEqual(j["daily_spent"], 42)
        self.assertEqual(j["last_resin"], 140)

if __name__ == "__main__":
    unittest.main()