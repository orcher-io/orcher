//! Task priority levels.

use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;

/// Priority level for a task.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TaskPriorityLevel {
    /// Background tasks.
    Lowest = 0,
    /// Non-critical tasks.
    Low = 1,
    /// Default for most tasks.
    #[default]
    Normal = 2,
    /// Important tasks.
    High = 3,
    /// Critical tasks.
    Highest = 4,
    /// System-critical tasks.
    Emergency = 5,
}

impl TaskPriorityLevel {
    pub fn value(&self) -> u8 {
        *self as u8
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Lowest => "lowest",
            Self::Low => "low",
            Self::Normal => "normal",
            Self::High => "high",
            Self::Highest => "highest",
            Self::Emergency => "emergency",
        }
    }

    pub fn from_value(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Lowest),
            1 => Some(Self::Low),
            2 => Some(Self::Normal),
            3 => Some(Self::High),
            4 => Some(Self::Highest),
            5 => Some(Self::Emergency),
            _ => None,
        }
    }
}

impl fmt::Display for TaskPriorityLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl PartialOrd for TaskPriorityLevel {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TaskPriorityLevel {
    fn cmp(&self, other: &Self) -> Ordering {
        self.value().cmp(&other.value())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskPriority {
    level: TaskPriorityLevel,
}

impl TaskPriority {
    pub fn new(level: TaskPriorityLevel) -> Self {
        Self { level }
    }

    pub fn level(&self) -> TaskPriorityLevel {
        self.level
    }
}

impl Default for TaskPriority {
    fn default() -> Self {
        Self::new(TaskPriorityLevel::default())
    }
}

impl fmt::Display for TaskPriority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.level)
    }
}

impl PartialOrd for TaskPriority {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TaskPriority {
    fn cmp(&self, other: &Self) -> Ordering {
        self.level.cmp(&other.level)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_priority_level_ordering() {
        assert!(TaskPriorityLevel::High > TaskPriorityLevel::Normal);
        assert!(TaskPriorityLevel::Emergency > TaskPriorityLevel::High);
        assert!(TaskPriorityLevel::Low < TaskPriorityLevel::Normal);
    }

    #[test]
    fn test_priority_level_values() {
        assert_eq!(TaskPriorityLevel::Lowest.value(), 0);
        assert_eq!(TaskPriorityLevel::Normal.value(), 2);
        assert_eq!(TaskPriorityLevel::Emergency.value(), 5);
    }

    #[test]
    fn test_priority_level_from_value() {
        assert_eq!(
            TaskPriorityLevel::from_value(2),
            Some(TaskPriorityLevel::Normal)
        );
        assert_eq!(
            TaskPriorityLevel::from_value(5),
            Some(TaskPriorityLevel::Emergency)
        );
        assert_eq!(TaskPriorityLevel::from_value(10), None);
    }

    #[test]
    fn test_task_priority_creation() {
        let priority = TaskPriority::new(TaskPriorityLevel::High);
        assert_eq!(priority.level(), TaskPriorityLevel::High);
    }

    #[test]
    fn test_task_priority_default() {
        let priority = TaskPriority::default();
        assert_eq!(priority.level(), TaskPriorityLevel::Normal);
    }

    #[test]
    fn test_task_priority_display() {
        let priority = TaskPriority::new(TaskPriorityLevel::Normal);
        assert_eq!(priority.to_string(), "normal");
    }

    #[test]
    fn test_task_priority_ordering() {
        let normal = TaskPriority::new(TaskPriorityLevel::Normal);
        let high = TaskPriority::new(TaskPriorityLevel::High);
        assert!(high > normal);
        assert!(normal < high);
    }
}
