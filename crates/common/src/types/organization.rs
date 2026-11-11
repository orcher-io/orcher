//! Organization domain types for multi-tenancy, roles, and membership.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Organization member role with hierarchical permissions.
///
/// Hierarchy: `Owner` > `Admin` > `Member` > `Guest`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OrganizationRole {
    /// Full control over organization.
    Owner,
    /// Administrative access (members, settings, billing).
    Admin,
    /// Standard member access.
    #[default]
    Member,
    /// Limited read-only access.
    Guest,
}

impl OrganizationRole {
    /// Returns `true` for `Owner` and `Admin` roles.
    pub fn is_admin(&self) -> bool {
        matches!(self, Self::Owner | Self::Admin)
    }

    pub fn is_owner(&self) -> bool {
        matches!(self, Self::Owner)
    }

    pub fn can_manage_members(&self) -> bool {
        matches!(self, Self::Owner | Self::Admin)
    }

    pub fn can_manage_settings(&self) -> bool {
        matches!(self, Self::Owner | Self::Admin)
    }

    pub fn can_manage_billing(&self) -> bool {
        matches!(self, Self::Owner)
    }

    pub fn can_delete_organization(&self) -> bool {
        matches!(self, Self::Owner)
    }

    pub fn can_invite_members(&self) -> bool {
        matches!(self, Self::Owner | Self::Admin | Self::Member)
    }

    pub fn can_create_resources(&self) -> bool {
        !matches!(self, Self::Guest)
    }

    pub fn can_modify_resources(&self) -> bool {
        !matches!(self, Self::Guest)
    }

    pub fn can_delete_resources(&self) -> bool {
        !matches!(self, Self::Guest)
    }

    /// Returns `true` if this role's hierarchy level meets or exceeds the required role.
    pub fn meets_requirement(&self, required: OrganizationRole) -> bool {
        self.hierarchy_level() >= required.hierarchy_level()
    }

    fn hierarchy_level(&self) -> u8 {
        match self {
            Self::Owner => 4,
            Self::Admin => 3,
            Self::Member => 2,
            Self::Guest => 1,
        }
    }

    pub fn all() -> Vec<Self> {
        vec![Self::Owner, Self::Admin, Self::Member, Self::Guest]
    }

    pub fn assignable_roles(&self) -> Vec<Self> {
        match self {
            Self::Owner => vec![Self::Admin, Self::Member, Self::Guest],
            Self::Admin => vec![Self::Member, Self::Guest],
            _ => vec![],
        }
    }
}

impl fmt::Display for OrganizationRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Owner => write!(f, "owner"),
            Self::Admin => write!(f, "admin"),
            Self::Member => write!(f, "member"),
            Self::Guest => write!(f, "guest"),
        }
    }
}

impl std::str::FromStr for OrganizationRole {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "owner" => Ok(Self::Owner),
            "admin" => Ok(Self::Admin),
            "member" => Ok(Self::Member),
            "guest" => Ok(Self::Guest),
            _ => Err(format!("Invalid organization role: {}", s)),
        }
    }
}

/// Organization lifecycle status.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OrganizationStatus {
    /// Organization is active and operational.
    #[default]
    Active,
    /// Organization is temporarily suspended.
    Suspended,
    /// Organization is soft-deleted (can be restored).
    Deleted,
    /// Organization is in trial period.
    Trial,
}

impl OrganizationStatus {
    pub fn is_active(&self) -> bool {
        matches!(self, Self::Active)
    }

    pub fn is_operational(&self) -> bool {
        matches!(self, Self::Active | Self::Trial)
    }

    pub fn is_suspended(&self) -> bool {
        matches!(self, Self::Suspended)
    }

    pub fn is_deleted(&self) -> bool {
        matches!(self, Self::Deleted)
    }
}

impl fmt::Display for OrganizationStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Active => write!(f, "active"),
            Self::Suspended => write!(f, "suspended"),
            Self::Deleted => write!(f, "deleted"),
            Self::Trial => write!(f, "trial"),
        }
    }
}

/// Organization subscription plan type.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OrganizationPlan {
    /// Free tier with basic features and limits.
    #[default]
    Free,
    /// Starter plan for small teams.
    Starter,
    /// Professional plan for growing teams.
    Professional,
    /// Enterprise plan with custom features and limits.
    Enterprise,
}

impl OrganizationPlan {
    pub fn default_max_users(&self) -> i32 {
        match self {
            Self::Free => 5,
            Self::Starter => 10,
            Self::Professional => 50,
            Self::Enterprise => 1000,
        }
    }

    pub fn default_max_tasks_per_month(&self) -> i32 {
        match self {
            Self::Free => 1000,
            Self::Starter => 10000,
            Self::Professional => 100000,
            Self::Enterprise => 1000000,
        }
    }

    pub fn default_storage_limit_mb(&self) -> i32 {
        match self {
            Self::Free => 1000,           // 1 GB
            Self::Starter => 10000,       // 10 GB
            Self::Professional => 100000, // 100 GB
            Self::Enterprise => 1000000,  // 1 TB
        }
    }

    pub fn supports_advanced_workflows(&self) -> bool {
        matches!(self, Self::Professional | Self::Enterprise)
    }

    pub fn has_priority_support(&self) -> bool {
        matches!(self, Self::Enterprise)
    }
}

impl fmt::Display for OrganizationPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Free => write!(f, "free"),
            Self::Starter => write!(f, "starter"),
            Self::Professional => write!(f, "professional"),
            Self::Enterprise => write!(f, "enterprise"),
        }
    }
}

/// Organization member status.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MemberStatus {
    /// Member is active.
    #[default]
    Active,
    /// Member is temporarily inactive.
    Inactive,
    /// Member is suspended by admin.
    Suspended,
}

impl MemberStatus {
    pub fn is_active(&self) -> bool {
        matches!(self, Self::Active)
    }
}

impl fmt::Display for MemberStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Active => write!(f, "active"),
            Self::Inactive => write!(f, "inactive"),
            Self::Suspended => write!(f, "suspended"),
        }
    }
}

/// Organization invitation status.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum InvitationStatus {
    /// Invitation is pending acceptance.
    #[default]
    Pending,
    /// Invitation was accepted.
    Accepted,
    /// Invitation was rejected by recipient.
    Rejected,
    /// Invitation has expired.
    Expired,
    /// Invitation was cancelled by sender.
    Cancelled,
}

impl InvitationStatus {
    pub fn is_pending(&self) -> bool {
        matches!(self, Self::Pending)
    }

    pub fn can_be_accepted(&self) -> bool {
        matches!(self, Self::Pending)
    }

    pub fn is_resolved(&self) -> bool {
        matches!(
            self,
            Self::Accepted | Self::Rejected | Self::Expired | Self::Cancelled
        )
    }
}

impl fmt::Display for InvitationStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pending => write!(f, "pending"),
            Self::Accepted => write!(f, "accepted"),
            Self::Rejected => write!(f, "rejected"),
            Self::Expired => write!(f, "expired"),
            Self::Cancelled => write!(f, "cancelled"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_role_hierarchy() {
        assert!(OrganizationRole::Owner.meets_requirement(OrganizationRole::Admin));
        assert!(OrganizationRole::Admin.meets_requirement(OrganizationRole::Member));
        assert!(OrganizationRole::Member.meets_requirement(OrganizationRole::Guest));

        assert!(!OrganizationRole::Guest.meets_requirement(OrganizationRole::Member));
        assert!(!OrganizationRole::Member.meets_requirement(OrganizationRole::Admin));
        assert!(!OrganizationRole::Admin.meets_requirement(OrganizationRole::Owner));
    }

    #[test]
    fn test_role_permissions() {
        let owner = OrganizationRole::Owner;
        let admin = OrganizationRole::Admin;
        let member = OrganizationRole::Member;
        let guest = OrganizationRole::Guest;

        // Admin privileges
        assert!(owner.is_admin());
        assert!(admin.is_admin());
        assert!(!member.is_admin());
        assert!(!guest.is_admin());

        // Manage members
        assert!(owner.can_manage_members());
        assert!(admin.can_manage_members());
        assert!(!member.can_manage_members());
        assert!(!guest.can_manage_members());

        // Manage billing
        assert!(owner.can_manage_billing());
        assert!(!admin.can_manage_billing());
        assert!(!member.can_manage_billing());
        assert!(!guest.can_manage_billing());

        // Create resources
        assert!(owner.can_create_resources());
        assert!(admin.can_create_resources());
        assert!(member.can_create_resources());
        assert!(!guest.can_create_resources());
    }

    #[test]
    fn test_role_from_str() {
        assert_eq!(
            "owner".parse::<OrganizationRole>().unwrap(),
            OrganizationRole::Owner
        );
        assert_eq!(
            "admin".parse::<OrganizationRole>().unwrap(),
            OrganizationRole::Admin
        );
        assert_eq!(
            "MEMBER".parse::<OrganizationRole>().unwrap(),
            OrganizationRole::Member
        );
        assert!("invalid".parse::<OrganizationRole>().is_err());
    }

    #[test]
    fn test_role_display() {
        assert_eq!(OrganizationRole::Owner.to_string(), "owner");
        assert_eq!(OrganizationRole::Admin.to_string(), "admin");
        assert_eq!(OrganizationRole::Member.to_string(), "member");
        assert_eq!(OrganizationRole::Guest.to_string(), "guest");
    }

    #[test]
    fn test_plan_limits() {
        let free = OrganizationPlan::Free;
        assert_eq!(free.default_max_users(), 5);
        assert_eq!(free.default_max_tasks_per_month(), 1000);
        assert!(!free.supports_advanced_workflows());

        let enterprise = OrganizationPlan::Enterprise;
        assert_eq!(enterprise.default_max_users(), 1000);
        assert!(enterprise.supports_advanced_workflows());
        assert!(enterprise.has_priority_support());
    }

    #[test]
    fn test_status_checks() {
        assert!(OrganizationStatus::Active.is_active());
        assert!(OrganizationStatus::Active.is_operational());
        assert!(!OrganizationStatus::Suspended.is_operational());

        assert!(InvitationStatus::Pending.can_be_accepted());
        assert!(!InvitationStatus::Accepted.can_be_accepted());
        assert!(InvitationStatus::Accepted.is_resolved());
    }

    #[test]
    fn test_role_serialization() {
        let role = OrganizationRole::Admin;
        let json = serde_json::to_string(&role).unwrap();
        assert_eq!(json, "\"admin\"");

        let deserialized: OrganizationRole = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, role);
    }

    #[test]
    fn test_assignable_roles() {
        let owner_assignable = OrganizationRole::Owner.assignable_roles();
        assert_eq!(owner_assignable.len(), 3);
        assert!(owner_assignable.contains(&OrganizationRole::Admin));
        assert!(!owner_assignable.contains(&OrganizationRole::Owner));

        let member_assignable = OrganizationRole::Member.assignable_roles();
        assert_eq!(member_assignable.len(), 0);
    }
}
