# Authentication QA Checklist — Manual Integration Tests

This checklist covers end-to-end verification of the authentication system
across backend, Flutter app, and React frontend. Run through each section
after deploying the auth changes to a staging or local environment.

---

## 1. Registration Flow

### Happy Path
- [ ] POST `/auth/register` with valid email + password returns 200 with `{user_id, email, token}`
- [ ] Token is a valid JWT with `sub` = user_id and `exp` ~30 days from now
- [ ] New user appears in the `users` table with bcrypt-hashed password
- [ ] Flutter: open app -> redirected to login -> tap "Register" -> fill form -> submit -> lands on home screen
- [ ] React: open browser -> redirected to /login -> click register link -> fill form -> submit -> lands on race list

### Edge Cases
- [ ] Registering with an already-used email returns 409 "Email already registered"
- [ ] Registering with invalid email format returns 422
- [ ] Registering with empty password returns 422
- [ ] Registering with empty body returns 422
- [ ] Very long email (>255 chars) is handled gracefully (422 or 400)
- [ ] Unicode characters in password work correctly (can log in afterwards)

---

## 2. Login Flow

### Happy Path
- [ ] POST `/auth/login` with correct email + password returns 200 with token
- [ ] Token works for subsequent authenticated API calls
- [ ] Flutter: enter credentials -> tap login -> home screen loads, race list visible
- [ ] React: enter credentials -> submit -> redirected to home, races load

### Edge Cases
- [ ] Wrong password returns 401 "Invalid email or password"
- [ ] Non-existent email returns 401 (same message, no user enumeration)
- [ ] Empty password returns 401
- [ ] SQL injection attempt in email field is handled safely (422 or 401)
- [ ] Multiple rapid login attempts work (no rate limit issues in current scope)

---

## 3. Token Expiry Behavior

- [ ] Token created with `ACCESS_TOKEN_EXPIRE_DAYS=30` has correct `exp` claim
- [ ] For testing: temporarily set `ACCESS_TOKEN_EXPIRE_DAYS=0` (or create a manually expired token)
  - [ ] Expired token on any protected endpoint returns 401
  - [ ] Flutter: expired token -> app redirects to login screen
  - [ ] React: expired token -> redirected to /login, localStorage token cleared

### Token Validation
- [ ] Token signed with wrong secret returns 401
- [ ] Malformed JWT string returns 401
- [ ] Token with missing `sub` claim returns 401
- [ ] Token for a deleted user returns 401
- [ ] `Authorization: Basic <token>` (wrong scheme) returns 401/403

---

## 4. Cross-User Isolation

### Race List Isolation
- [ ] Register User A, upload a race via Flutter -> User A sees 1 race in list
- [ ] Register User B, upload a different race -> User B sees only their 1 race
- [ ] User A's race list still shows only 1 race (not 2)
- [ ] `GET /race/list` returns only the authenticated user's races
- [ ] `GET /race/list_ids` returns only the authenticated user's race IDs

### Per-Race Ownership
- [ ] User A gets 200 on `GET /race/{own_race_id}/status`
- [ ] User A gets 404 on `GET /race/{user_b_race_id}/status` (not 403)
- [ ] User A gets 404 on `GET /race/{user_b_race_id}/download`
- [ ] User A gets 404 on `DELETE /race/{user_b_race_id}`
- [ ] User A gets 404 on `GET /race/{user_b_race_id}/compare/1/2`
- [ ] User A gets 404 on `GET /race/{user_b_race_id}/fuel/1`
- [ ] Truly non-existent race_id also returns 404 (same behavior)

---

## 5. Redis Key Namespacing

### Setup
- [ ] User A completes a lap in AMS2 -> `POST /race/last-lap` with User A's token
- [ ] User B completes a lap -> `POST /race/last-lap` with User B's token

### Verification
- [ ] Redis key `telemetry:{user_a_id}:last_lap` exists with User A's data
- [ ] Redis key `telemetry:{user_b_id}:last_lap` exists with User B's data
- [ ] Redis key `telemetry:{user_a_id}:fastest_lap` is independent from User B's
- [ ] Redis key `telemetry:{user_a_id}:last_lap_audio` is User A's audio URL
- [ ] Old global keys (`telemetry:last_lap`, `telemetry:fastest_lap`) are NOT written to
- [ ] User A's fastest lap is compared only against User A's previous fastest
- [ ] `GET /race/last-lap/audio` returns audio for the authenticated user only

---

## 6. Change Password Flow

### Happy Path
- [ ] Logged-in user calls `POST /auth/change-password` with correct current password -> 200
- [ ] Can log in with the new password
- [ ] Cannot log in with the old password

### Edge Cases
- [ ] Wrong current_password returns 401
- [ ] Email mismatch (email != authenticated user's email) returns 403
- [ ] No auth token returns 401
- [ ] Expired auth token returns 401
- [ ] Flutter: Settings -> Change Password -> fill form -> success snackbar -> pop back
- [ ] React: /change-password page -> fill form -> success message

---

## 7. Logout from All Clients

- [ ] Flutter: tap logout -> redirected to login screen
- [ ] Flutter: after logout, stored token is deleted from secure storage
- [ ] Flutter: back button after logout does NOT show authenticated content
- [ ] React: click logout -> redirected to /login
- [ ] React: after logout, localStorage `token` key is removed
- [ ] React: manually navigating to / after logout redirects to /login

---

## 8. Migration Verification (Existing Data Preserved)

### Pre-migration State
- [ ] Note the count of existing races in the database
- [ ] Note the race_ids of existing races

### Post-migration
- [ ] `users` table exists with correct schema (id, email, hashed_password, created_at)
- [ ] Default user created with email `blort.music@gmail.com`
- [ ] Can log in as default user with password `changeme1234`
- [ ] All existing races now have `user_id` set to the default user's ID
- [ ] `SELECT COUNT(*) FROM races WHERE user_id IS NULL` returns 0
- [ ] Race count matches pre-migration count (no data lost)
- [ ] All existing race endpoints still work when authenticated as default user
- [ ] Lap comparison, fuel analysis, downloads all function normally

---

## 9. ProtectedRoute (React Frontend)

- [ ] Unauthenticated visit to `/` redirects to `/login`
- [ ] Unauthenticated visit to `/races` redirects to `/login`
- [ ] Unauthenticated visit to `/fuel` redirects to `/login`
- [ ] Authenticated visit to `/login` redirects to `/`
- [ ] Loading state shows spinner (brief flash during token check)
- [ ] After token expiry, next navigation triggers redirect to /login

---

## 10. Concurrency / Race Conditions

- [ ] Two users uploading races simultaneously -> each race gets correct user_id
- [ ] Two users submitting last-lap simultaneously -> each gets own Redis keys
- [ ] Rapid login/logout cycles do not corrupt local token state
- [ ] Password change during active session on another device -> old token still works until expiry (stateless JWT)

---

## 11. Environment Variables

- [ ] `JWT_SECRET_KEY` is set on Railway (production) — NOT a test value
- [ ] `JWT_SECRET_KEY` is different from any value in source control
- [ ] App fails to start if `JWT_SECRET_KEY` is not set (no silent fallback)
- [ ] `ACCESS_TOKEN_EXPIRE_DAYS` defaults to 30 if not set
- [ ] `JWT_ALGORITHM` defaults to HS256 if not set
