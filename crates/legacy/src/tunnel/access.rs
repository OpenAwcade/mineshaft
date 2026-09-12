use std::collections::HashSet;

#[derive(Clone, Debug, Default)]
pub struct AccessControl {
    blocked_players: HashSet<u32>,
    followers_only: bool,
    allowed_followers: HashSet<u32>,
}

impl AccessControl {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn block_player(&mut self, client_id: u32) {
        self.blocked_players.insert(client_id);
    }

    pub fn unblock_player(&mut self, client_id: u32) {
        self.blocked_players.remove(&client_id);
    }

    pub fn is_blocked(&self, client_id: u32) -> bool {
        self.blocked_players.contains(&client_id)
    }

    pub fn set_followers_only(&mut self, enabled: bool) {
        self.followers_only = enabled;
    }

    pub fn add_follower(&mut self, client_id: u32) {
        self.allowed_followers.insert(client_id);
    }

    pub fn remove_follower(&mut self, client_id: u32) {
        self.allowed_followers.remove(&client_id);
    }

    pub fn is_allowed(&self, client_id: u32) -> bool {
        if self.blocked_players.contains(&client_id) {
            return false;
        }
        if self.followers_only && !self.allowed_followers.contains(&client_id) {
            return false;
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_access_control() {
        let mut ac = AccessControl::new();
        assert!(ac.is_allowed(123));

        ac.block_player(123);
        assert!(!ac.is_allowed(123));
        assert!(ac.is_blocked(123));

        ac.unblock_player(123);
        assert!(ac.is_allowed(123));

        ac.set_followers_only(true);
        assert!(!ac.is_allowed(456));

        ac.add_follower(456);
        assert!(ac.is_allowed(456));
    }
}
