---
title: ForwardBaseSpawner (API)
---

# ForwardBaseSpawner

API reference for the `forwardbasespawner` package.

## Spawner

::: forwardbasespawner.forward.ForwardBaseSpawner
    options:
      members:
        - port_forward_info
        - start_id
        - call_during_startup
        - already_stopped
        - already_post_stop_hooked
        - extra_labels
        - custom_port
        - ssh_recreate_at_start
        - ssh_during_startup
        - ssh_key
        - ssh_remote_key
        - ssh_username
        - ssh_remote_username
        - ssh_node
        - svc_create
        - ssh_node_mapping
        - ssh_remote_node
        - ssh_port
        - ssh_remote_port
        - ssh_custom_forward_remote
        - ssh_custom_forward_remote_remove
        - pre_stop_hook
        - pre_poll_hook
        - update_expected_path
        - update_start_response
        - ssh_create_remote_forward
        - ssh_custom_forward
        - ssh_custom_forward_remove
        - ssh_custom_svc
        - ssh_custom_svc_remove
        - ssh_forward_options
        - ssh_forward_remote_options
        - filter_events
        - cancelling_event
        - show_first_default_event
        - public_api_url
        - dns_name_template
        - svc_name_template
        - namespace
        - get_state
        - load_state
        - clear_state
        - get_env
        - get_extra_labels
        - start
        - poll
        - stop
        - run_ssh_forward
        - run_ssh_forward_remove
        - run_ssh_forward_remote_remove
        - ssh_default_forward
        - ssh_default_forward_remove
        - ssh_default_forward_remote
        - ssh_default_forward_remote_remove
        - ssh_default_svc
        - ssh_default_svc_remove
        - split_service_address
        - subprocess_cmd
        - get_forward_cmd
        - get_forward_remote_cmd

## API handlers

::: forwardbasespawner.api_events.SpawnEventsAPIHandler

::: forwardbasespawner.api_setup_tunnel.SetupTunnelAPIHandler

::: forwardbasespawner.api_list_servers.ListServersAPIHandler

::: forwardbasespawner.api_sshnode_restarted.SSHNodeRestartedAPIHandler
