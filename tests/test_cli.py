import unittest
import subprocess
import sys
import os
from io import StringIO
import time
import threading
from queue import Queue

class TestCLISystem(unittest.TestCase):
    def setUp(self):
        # Get the absolute path to the CLI script
        self.cli_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'CLI', 'cli.py'))
    
    def run_cli_commands(self, commands):
        """Run a series of commands through the CLI and return the output"""
        # Start the CLI process
        process = subprocess.Popen(
            [sys.executable, self.cli_path],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            universal_newlines=True
        )
        
        output = []
        # Send each command and collect output
        for cmd in commands:
            process.stdin.write(cmd + '\n')
            process.stdin.flush()
            # Give the process time to process the command
            time.sleep(0.1)
        
        # Send exit command
        process.stdin.write('exit\n')
        process.stdin.flush()
        
        # Get the output
        stdout, stderr = process.communicate()
        return stdout, stderr

    def test_basic_transaction(self):
        """Test basic transaction operations through CLI"""
        commands = [
            'begin',
            'insert A test_value',
            'query',
            'commit'
        ]
        
        stdout, stderr = self.run_cli_commands(commands)
        
        # Verify the output contains expected responses
        self.assertIn('Started transaction', stdout)
        self.assertIn('Inserted A with value', stdout)
        self.assertIn('test_value', stdout)
        self.assertIn('Committed transaction', stdout)
        self.assertEqual('', stderr)  # No errors should be present

    def test_transaction_isolation(self):
        """Test transaction isolation through CLI using multiple transactions"""
        commands = [
            'begin',  # Transaction 1
            'insert A first_value',
            'commit',
            'begin',  # Transaction 2
            'update A second_value',
            'query',  # Should show second_value
            'commit',
            'begin',  # Transaction 3
            'query',  # Should show final state
            'commit'
        ]
        
        stdout, stderr = self.run_cli_commands(commands)
        
        # Verify the transaction progression
        self.assertIn('first_value', stdout)
        self.assertIn('second_value', stdout)
        self.assertEqual('', stderr)  # No errors should be present

    def test_true_concurrent_transactions(self):
        """Test truly concurrent transactions with multiple users accessing the database simultaneously"""
        def user1_session(output_queue):
            """First user tries to insert and update records"""
            commands = [
                'begin',
                'insert A user1_value',
                'insert B also_from_user1',
                # Delay to let user2 read during our transaction
                'query',
                'commit'
            ]
            stdout, stderr = self.run_cli_commands(commands)
            output_queue.put(('user1', stdout, stderr))

        def user2_session(output_queue):
            """Second user reads and updates while user1 is working"""
            # Small delay to ensure user1 has started
            time.sleep(0.2)
            commands = [
                'begin',
                'query',  # Should not see user1's uncommitted changes
                'insert C user2_value',
                'commit'
            ]
            stdout, stderr = self.run_cli_commands(commands)
            output_queue.put(('user2', stdout, stderr))

        def user3_session(output_queue):
            """Third user tries to update record A while user1 is still working"""
            time.sleep(0.3)
            commands = [
                'begin',
                'update A user3_update',  # This should wait for user1's commit
                'query',
                'commit'
            ]
            stdout, stderr = self.run_cli_commands(commands)
            output_queue.put(('user3', stdout, stderr))

        # Create threads for each user
        output_queue = Queue()
        threads = [
            threading.Thread(target=user1_session, args=(output_queue,)),
            threading.Thread(target=user2_session, args=(output_queue,)),
            threading.Thread(target=user3_session, args=(output_queue,))
        ]

        # Start all threads
        for thread in threads:
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Collect all outputs
        outputs = {}
        while not output_queue.empty():
            user, stdout, stderr = output_queue.get()
            outputs[user] = (stdout, stderr)

        # Verify the concurrent behavior
        user1_out, user1_err = outputs['user1']
        user2_out, user2_err = outputs['user2']
        user3_out, user3_err = outputs['user3']

        # User 2's query should not see User 1's uncommitted changes
        self.assertNotIn('user1_value', user2_out)
        
        # User 3's update should be blocked or fail if User 1 hasn't committed
        self.assertTrue(
            'Error: cannot update record' in user3_out or
            'waiting for transaction' in user3_out or
            'Transaction blocked' in user3_out
        )
        
        # Final state check
        final_commands = [
            'begin',
            'query',
            'commit'
        ]
        final_stdout, final_stderr = self.run_cli_commands(final_commands)
        
        # Should see the final state with all changes
        self.assertIn('user3_update', final_stdout)  # A's final value
        self.assertIn('also_from_user1', final_stdout)  # B's value
        self.assertIn('user2_value', final_stdout)  # C's value

    def test_error_handling(self):
        """Test error handling in CLI"""
        commands = [
            'insert A value',  # Should fail - no active transaction
            'begin',
            'insert A first_value',
            'insert A first_value',  # Should fail - duplicate ID
            'commit'
        ]
        
        stdout, stderr = self.run_cli_commands(commands)
        
        # Verify error messages
        self.assertIn('Error: No transaction in progress', stdout)
        self.assertIn('Error: record with ID A already exists', stdout)

    def test_mvcc_snapshot_isolation(self):
        """Test MVCC snapshot isolation through CLI interface"""
        commands = [
            # Transaction 1: Insert A
            'begin',
            'insert A value_1',
            'commit',
            
            # Transaction 2: Insert B
            'begin',
            'insert B value_2',
            'commit',
            
            # Transaction 3: Update A (but don't commit)
            'begin',
            'update A value_3',
            
            # Transaction 4: Read snapshot (should see old versions)
            'begin',
            'query',  # Should show A with value_1 and B with value_2
            'commit',
            
            # Now commit Transaction 3's update
            'commit',
            
            # Transaction 5: Read again (should see new versions)
            'begin',
            'query',  # Should show A with value_3 and B with value_2
            'commit'
        ]
        
        stdout, stderr = self.run_cli_commands(commands)
        
        # Check version progression
        output_lines = stdout.split('\n')
        
        # Find the two query results sections
        query_results = [i for i, line in enumerate(output_lines) if 'Query Results' in line]
        
        # First query should show original values
        first_query_section = '\n'.join(output_lines[query_results[0]:query_results[1]])
        self.assertIn('A → value_1', first_query_section)
        self.assertIn('B → value_2', first_query_section)
        
        # Second query should show updated values
        second_query_section = '\n'.join(output_lines[query_results[1]:])
        self.assertIn('A → value_3', second_query_section)
        self.assertIn('B → value_2', second_query_section)
        
        self.assertEqual('', stderr)  # No errors should be present

if __name__ == '__main__':
    unittest.main() 