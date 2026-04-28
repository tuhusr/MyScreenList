ScreenList Capacitor iOS Prep

What this setup does

- Wraps the deployed ScreenList site inside a native iOS Capacitor shell.
- Uses the live Workers URL instead of bundling the site locally.
- Keeps your current `/api/tmdb` and `/api/rawg` routes working without rewriting the app.

Files added

- `package.json`: Capacitor dependencies and helper scripts.
- `capacitor.config.json`: Native app config for iOS.
- `capacitor-web/`: Minimal placeholder web directory required by Capacitor while the app loads the live site.

Important assumption

- Bundle ID is set to `dev.koomgaming.screenlist`.
- If you already know your final App Store bundle identifier, change `appId` in `capacitor.config.json` before publishing.

Recommended next steps after install

1. Run `npm install`
2. Run `npx cap add ios`
3. Run `npx cap sync ios`
4. Run `npx cap open ios`
5. In Xcode:
   - Set your Apple Team
   - Confirm the bundle identifier
   - Add app icons / splash assets
   - Archive and upload to App Store Connect

Notes

- Because this app loads the deployed site, any web updates you push to Workers will show in the iOS app without rebundling the full web app.
- For App Store review, make sure your deployed production site is stable and mobile-friendly.
