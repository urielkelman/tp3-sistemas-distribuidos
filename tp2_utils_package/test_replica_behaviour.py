BULLY_LAYER = "BULLY"
CONNECTION_LAYER = "CONNECTION"

# class TestReplicaBehaviour(unittest.TestCase):
#     TEST_PORT_1 = random.randint(8000, 9000)
#     TEST_PORT_2 = random.randint(7000, 8000)
#
#     @staticmethod
#     def _establish_socket_connection(host: str, port: int):
#         established = False
#         while not established:
#             try:
#                 connection = socket.create_connection((host, port))
#                 return connection
#             except Exception:
#                 traceback.print_exc()
#                 established = False
#
#     @staticmethod
#     def _launch_process_with_bully(port: int, bully_process_id: int, bully_processes_ids: List[int],
#                                    messages_queue: Queue, queued_message_semaphore: Optional[Semaphore],
#                                    receive_messages=True):
#         bully_leader_election = BullyLeaderElection(bully_process_id, bully_processes_ids)
#         messages = bully_leader_election.start_election()
#         messages_queue.put({"layer": BULLY_LAYER, "message": messages[len(messages) - 1], "host_id": bully_process_id})
#         if queued_message_semaphore:
#             queued_message_semaphore.release()
#
#         if receive_messages:
#             sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#             sock.bind(('', port))
#             sock.listen(5)
#             connection, address = sock.accept()
#             received_message = JsonReceiver.receive_json(connection)
#             response_message = bully_leader_election.receive_message(received_message["message"])
#             if response_message:
#                 JsonSender.send_json(connection,
#                                      {"layer": BULLY_LAYER, "message": response_message, "host_id": bully_process_id})
#             else:
#                 JsonSender.send_json(connection,
#                                      {"layer": CONNECTION_LAYER, "message": "ACK", "host_id": bully_process_id})
#             sock.close()
#
#     def setUp(self) -> None:
#         try:
#             from pytest_cov.embed import cleanup_on_sigterm
#         except ImportError:
#             pass
#         else:
#             cleanup_on_sigterm()
#
#     def set_up_two_nodes_use_first(self, port) -> None:
#         self.incoming_messages_queue_1 = Queue()
#         other_bully_process = Process(target=self._launch_process_with_bully,
#                                       args=(port, 2, [1, 2], self.incoming_messages_queue_1, None,))
#         other_bully_process.start()
#
#         self.connection = self._establish_socket_connection("localhost", port)
#
#         self.outcoming_messages_queue = Queue()
#
#         self.bully_leader_election = BullyLeaderElection(1, [1, 2])
#         self.replica_behaviour = ReplicaBehaviour({2: self.connection}, self.bully_leader_election,
#                                                   self.incoming_messages_queue_1,
#                                                   {2: self.outcoming_messages_queue})
#
#     def set_up_two_nodes_use_second(self, port) -> None:
#         self.incoming_messages_queue_1 = Queue()
#         other_bully_process = Process(target=self._launch_process_with_bully,
#                                       args=(port, 1, [1, 2], self.incoming_messages_queue_1, None,))
#         other_bully_process.start()
#
#         connection = self._establish_socket_connection("localhost", self.TEST_PORT_1)
#
#         self.outcoming_messages_queue = Queue()
#
#         self.bully_leader_election = BullyLeaderElection(2, [1, 2])
#         self.replica_behaviour = ReplicaBehaviour({1: connection}, self.bully_leader_election,
#                                                   self.incoming_messages_queue_1,
#                                                   {1: self.outcoming_messages_queue})
#
#     def test_one_node_initialization(self):
#         bully_leader_election = BullyLeaderElection(1, [1])
#         replica_behaviour = ReplicaBehaviour({}, bully_leader_election, Queue(), {})
#         replica_behaviour.execute_tasks()
#
#         self.assertEqual(bully_leader_election.get_current_leader(), 1)
#
#     def test_initialization_of_lower_node(self):
#         self.set_up_two_nodes_use_first(self.TEST_PORT_1)
#         self.replica_behaviour.execute_tasks()
#         ack_message = self.outcoming_messages_queue.get()
#         self.assertEqual(self.bully_leader_election.get_current_leader(), 2)
#         self.assertEqual(ack_message["message"], "ACK")
#         self.connection.close()
#
#     def test_initialization_of_bigger_node(self):
#         self.set_up_two_nodes_use_second(self.TEST_PORT_1)
#         self.replica_behaviour.execute_tasks()
#
#         response_to_leader_message = self.outcoming_messages_queue.get()
#
#         self.assertEqual(self.bully_leader_election.get_current_leader(), 2)
#         self.assertEqual(response_to_leader_message["message"]["message"], "LEADER")
#
#     def test_two_nodes_the_second_goes_down_and_the_first_take_the_leadership(self):
#         self.set_up_two_nodes_use_first(self.TEST_PORT_1)
#         self.replica_behaviour.execute_tasks()
#         self.outcoming_messages_queue.get()
#
#         # We know that node 2 is the leader. But the method with the mocked bully only respond to one message and close the socket, so if we
#         # execute the replica behaviour again, it should detect that node 2 is down.
#         self.replica_behaviour.execute_tasks()
#
#         self.assertEqual(self.bully_leader_election.get_current_leader(), 1)
#         self.connection.close()
#
#     def test_two_node_the_second_goes_down_and_restarts(self):
#         self.set_up_two_nodes_use_first(self.TEST_PORT_1)
#         self.replica_behaviour.execute_tasks()
#         self.outcoming_messages_queue.get()
#         self.replica_behaviour.execute_tasks()
#
#         # Node 1 has the leadership. Now, the behaviour receives a message indicating that node 2 has restarted.
#         queued_message_semaphore = Semaphore(0)
#         restarted_node_process = Process(target=self._launch_process_with_bully,
#                                          args=(self.TEST_PORT_1, 2, [1, 2], self.incoming_messages_queue_1,
#                                                queued_message_semaphore,))
#         restarted_node_process.start()
#         queued_message_semaphore.acquire()
#         self.replica_behaviour.execute_tasks()
#         self.assertEqual(self.bully_leader_election.get_current_leader(), 2)
#         self.connection.close()
#         # We kill the process because if not it loops to the infinity, waiting for jsons.
#         restarted_node_process.terminate()
#
#     def test_two_nodes_the_first_goes_down_and_restarts(self):
#         self.set_up_two_nodes_use_second(self.TEST_PORT_1)
#         self.replica_behaviour.execute_tasks()
#
#         # We know that the second node has the leadership. First node restarts and send an election message.
#         restarted_node_process = Process(target=self._launch_process_with_bully,
#                                          args=(self.TEST_PORT_1, 1, [1, 2], self.incoming_messages_queue_1, None,))
#         restarted_node_process.start()
#
#         self.replica_behaviour.execute_tasks()
#
#         response_to_leader_message = self.outcoming_messages_queue.get()
#
#         self.assertEqual(self.bully_leader_election.get_current_leader(), 2)
#         self.assertEqual(response_to_leader_message["message"]["message"], "LEADER")
#         restarted_node_process.terminate()
#
#     def test_three_nodes_two_of_them_goes_down_and_restarts(self):
#         incoming_messages_queue = Queue()
#         node_1 = Process(target=self._launch_process_with_bully,
#                          args=(self.TEST_PORT_1, 1, [1, 2, 3], incoming_messages_queue, None,))
#         node_1.start()
#         node_2 = Process(target=self._launch_process_with_bully,
#                          args=(self.TEST_PORT_2, 2, [1, 2, 3], incoming_messages_queue, None,))
#         node_2.start()
#
#         connection_1 = self._establish_socket_connection("localhost", self.TEST_PORT_1)
#         connection_2 = self._establish_socket_connection("localhost", self.TEST_PORT_2)
#         outcoming_messages_queue_1 = Queue()
#         outcoming_messages_queue_2 = Queue()
#
#         bully_leader_election = BullyLeaderElection(3, [1, 2, 3])
#         replica_behaviour = ReplicaBehaviour({1: connection_1, 2: connection_2}, bully_leader_election,
#                                              incoming_messages_queue,
#                                              {1: outcoming_messages_queue_1, 2: outcoming_messages_queue_2})
#
#         replica_behaviour.execute_tasks()
#
#         node_1.terminate()
#         node_2.terminate()
#
#         # We know that node 3 is the leader.
#         node_restarted_1 = Process(target=self._launch_process_with_bully,
#                                    args=(self.TEST_PORT_1, 1, [1, 2, 3], incoming_messages_queue, None, False))
#         node_restarted_1.start()
#         node_restarted_2 = Process(target=self._launch_process_with_bully,
#                                    args=(self.TEST_PORT_2, 2, [1, 2, 3], incoming_messages_queue, None, False))
#         node_restarted_2.start()
#         replica_behaviour.execute_tasks()
#
#         self.assertEqual(bully_leader_election.get_current_leader(), 3)
#         response_message_1 = outcoming_messages_queue_1.get()
#         self.assertEqual(response_message_1["message"]["message"], "LEADER")
#         response_message_2 = outcoming_messages_queue_2.get()
#         self.assertEqual(response_message_2["message"]["message"], "LEADER")
#
#     def tearDown(self) -> None:
#         TestReplicaBehaviour.TEST_PORT_1 += 1
#         TestReplicaBehaviour.TEST_PORT_2 += 1
