#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# Copyright (C) 2025 New Vector, Ltd
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# See the GNU Affero General Public License for more details:
# <https://www.gnu.org/licenses/agpl-3.0.html>.
#

from synapse.replication.tcp.streams._base import (
    _STREAM_UPDATE_TARGET_ROW_COUNT,
    ThreadSubscriptionsStream,
)

from tests.replication._base import BaseStreamTestCase


class ThreadSubscriptionsStreamTestCase(BaseStreamTestCase):
    def test_thread_subscription_updates(self) -> None:
        """Test replication with thread subscription updates"""
        store = self.hs.get_datastores().main

        # Create thread subscription updates
        updates = []
        room_id = "!test_room:example.com"

        # Generate several thread subscription updates
        for i in range(_STREAM_UPDATE_TARGET_ROW_COUNT + 5):
            thread_root_id = f"$thread_{i}:example.com"
            self.get_success(
                store.subscribe_user_to_thread(
                    "@test_user:example.org",
                    room_id,
                    thread_root_id,
                    automatic=True,
                )
            )
            updates.append(thread_root_id)

        # Also add one in a different room
        other_room_id = "!other_room:example.com"
        other_thread_root_id = "$other_thread:example.com"
        self.get_success(
            store.subscribe_user_to_thread(
                "@test_user:example.org",
                other_room_id,
                other_thread_root_id,
                automatic=False,
            )
        )

        # Tell the notifier to catch up to avoid duplicate rows
        self.replicate()

        # Check we're testing what we think we are: no rows should yet have been received
        self.assertEqual([], self.test_handler.received_rdata_rows)

        # Now reconnect to pull the updates
        self.reconnect()
        self.replicate()

        # We should have received all the expected rows in the right order
        received_rows = self.test_handler.received_rdata_rows

        # Verify all the thread subscription updates
        for thread_id in updates:
            (stream_name, token, row) = received_rows.pop(0)
            self.assertEqual(stream_name, ThreadSubscriptionsStream.NAME)
            self.assertIsInstance(row, ThreadSubscriptionsStream.ROW_TYPE)
            self.assertEqual(row.user_id, "@test_user:example.org")
            self.assertEqual(row.room_id, room_id)
            self.assertEqual(row.event_id, thread_id)

        # Verify the last update in the different room
        (stream_name, token, row) = received_rows.pop(0)
        self.assertEqual(stream_name, ThreadSubscriptionsStream.NAME)
        self.assertIsInstance(row, ThreadSubscriptionsStream.ROW_TYPE)
        self.assertEqual(row.user_id, "@test_user:example.org")
        self.assertEqual(row.room_id, other_room_id)
        self.assertEqual(row.event_id, other_thread_root_id)

        self.assertEqual([], received_rows)

    def test_multiple_users_thread_subscription_updates(self) -> None:
        """Test replication with thread subscription updates for multiple users"""
        store = self.hs.get_datastores().main
        room_id = "!test_room:example.com"
        thread_root_id = "$thread_root:example.com"

        # Create updates for multiple users
        users = ["@user1:example.com", "@user2:example.com", "@user3:example.com"]
        for user_id in users:
            self.get_success(
                store.subscribe_user_to_thread(
                    user_id, room_id, thread_root_id, automatic=True
                )
            )

        # Tell the notifier to catch up to avoid duplicate rows
        self.replicate()

        # Check no rows have been received yet
        self.assertEqual([], self.test_handler.received_rdata_rows)

        # Now reconnect to pull the updates
        self.reconnect()
        self.replicate()

        # We should have received all the expected rows
        received_rows = self.test_handler.received_rdata_rows

        # Should have one update per user
        self.assertEqual(len(received_rows), len(users))

        # Verify all updates
        for i, user_id in enumerate(users):
            (stream_name, token, row) = received_rows[i]
            self.assertEqual(stream_name, ThreadSubscriptionsStream.NAME)
            self.assertIsInstance(row, ThreadSubscriptionsStream.ROW_TYPE)
            self.assertEqual(row.user_id, user_id)
            self.assertEqual(row.room_id, room_id)
            self.assertEqual(row.event_id, thread_root_id)
