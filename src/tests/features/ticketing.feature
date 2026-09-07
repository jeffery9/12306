Feature: 12306 High-Concurrency Ticketing MVP BDD Acceptance
  As a railway passenger
  I want to query and book sub-route train tickets safely
  So that seat inventory remains strictly consistent without over-selling or deadlocks

  Scenario: Successful sub-route seat reservation
    Given a clean ticketing system with train "G666" and Stations "北京", "天津", "上海"
    And a seat with class "BUSINESS" is fully available
    When passenger requests to reserve a ticket from sequence 1 to 2
    Then the system should grant a reservation ID
    And the MySQL seat segment 1 should be marked as "HELD"
    And the Redis seat mask should reflect the reservation

  Scenario: Reject overlapping sub-route booking
    Given a passenger has already reserved a ticket from sequence 1 to 2
    When another passenger attempts to reserve a ticket from sequence 2 to 3
    And another passenger attempts to reserve an overlapping ticket from sequence 1 to 3
    Then the non-overlapping booking should succeed
    And the overlapping booking should be rejected as "No seats available"

  Scenario: Payment confirmation triggers eventual consistency
    Given a passenger has successfully reserved a ticket from sequence 1 to 2
    And they have established an order for that reservation
    When they complete payment for the order
    And the background event processor consumes the "ORDER_PAID" event
    Then the MySQL seat segment 1 should be "CONFIRMED"
    And the Redis query model for route 1 to 3 should return 0 available seats
