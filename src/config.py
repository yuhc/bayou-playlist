#!/usr/bin/python3

class Config:

    network_log          = False
    network_debug_socket = False

    server_log           = False
    server_detail_log    = False
    server_lock_log      = False

    client_log           = False

    master_log           = False
    master_cmd           = True

    stabilize_time       = 5
    start_server_time    = 0.3

    anti_entropy_lower   = 5  # divided by 100
    anti_entropy_upper   = 10
