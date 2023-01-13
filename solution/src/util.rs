use crate::domain::*;
use uuid::Uuid;

#[inline(always)]
pub(crate) fn not_leader_command(leader_hint: Option<Uuid>) -> ClientRequestResponse {
    ClientRequestResponse::CommandResponse(CommandResponseArgs {
        client_id: todo!(),
        sequence_num: todo!(),
        content: CommandResponseContent::NotLeader { leader_hint },
    })
}

#[inline(always)]
pub(crate) fn not_leader_add_server(leader_hint: Option<Uuid>) -> ClientRequestResponse {
    ClientRequestResponse::AddServerResponse(AddServerResponseArgs {
        new_server: todo!(),
        content: AddServerResponseContent::NotLeader { leader_hint },
    })
}

#[inline(always)]
pub(crate) fn not_leader_remove_server(leader_hint: Option<Uuid>) -> ClientRequestResponse {
    ClientRequestResponse::RemoveServerResponse(RemoveServerResponseArgs {
        old_server: todo!(),
        content: RemoveServerResponseContent::NotLeader { leader_hint },
    })
}

#[inline(always)]
pub(crate) fn not_leader_register_client(leader_hint: Option<Uuid>) -> ClientRequestResponse {
    ClientRequestResponse::RegisterClientResponse(RegisterClientResponseArgs {
        content: RegisterClientResponseContent::NotLeader { leader_hint },
    })
}
