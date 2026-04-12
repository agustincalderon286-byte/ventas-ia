import SwiftUI
#if canImport(UIKit)
import UIKit
#endif

@MainActor
struct ContentView: View {
  @StateObject private var store = CRMStore()
  @EnvironmentObject private var pushManager: PushNotificationManager
  @Environment(\.scenePhase) private var scenePhase

  var body: some View {
    Group {
      if shouldShowRootLoading {
        CRMLoadingView(
          title: store.isLoggingIn ? "Opening lead inbox..." : "Opening Agustin 2.0 CRM...",
          message: store.isLoggingIn
            ? "Checking your session and preparing the lead inbox."
            : "Loading your lead operations workspace."
        )
      } else if store.isAuthenticated {
        CRMWorkspaceView(store: store)
      } else {
        CRMLoginView(store: store)
      }
    }
    .task {
      await pushManager.prepareIfNeeded()
      await store.bootstrapIfNeeded()
      pushManager.updateAuthentication(isAuthenticated: store.isAuthenticated)
    }
    .onChange(of: store.isAuthenticated) { _, isAuthenticated in
      pushManager.updateAuthentication(isAuthenticated: isAuthenticated)
    }
    .onChange(of: scenePhase) { _, phase in
      guard phase == .active else {
        return
      }

      Task {
        await pushManager.refreshAuthorizationStatus()
        pushManager.updateAuthentication(isAuthenticated: store.isAuthenticated)
      }
    }
  }

  private var shouldShowRootLoading: Bool {
    if store.isBootstrapping || store.isLoggingIn {
      return true
    }

    return store.isAuthenticated && store.dashboard == nil && store.isRefreshingDashboard
  }
}

private struct CRMWorkspaceView: View {
  @ObservedObject var store: CRMStore
  @Environment(\.scenePhase) private var scenePhase
  @State private var selectedTab: WorkspaceTab = .crm

  private enum WorkspaceTab {
    case crm
    case applicants
    case agenda
  }

  var body: some View {
    TabView(selection: $selectedTab) {
      CRMInboxView(store: store)
        .tabItem {
          Label("CRM", systemImage: "tray.full.fill")
        }
        .tag(WorkspaceTab.crm)

      CRMApplicantsView(store: store)
        .tabItem {
          Label("Applicants", systemImage: "person.3.fill")
        }
        .tag(WorkspaceTab.applicants)

      CRMAgendaView(store: store)
        .tabItem {
          Label("Agenda", systemImage: "calendar")
        }
        .tag(WorkspaceTab.agenda)
    }
    .task(id: autoRefreshEnabled) {
      await autoRefreshWorkspaceLoop()
    }
  }

  private var autoRefreshEnabled: Bool {
    store.isAuthenticated && scenePhase == .active
  }

  private func autoRefreshWorkspaceLoop() async {
    guard autoRefreshEnabled else {
      return
    }

    await store.refreshWorkspace()

    while !Task.isCancelled {
      do {
        try await Task.sleep(for: .seconds(AppConfig.inboxAutoRefreshInterval))
      } catch {
        return
      }

      guard autoRefreshEnabled else {
        return
      }

      await store.refreshWorkspace()
    }
  }
}

private struct CRMLoadingView: View {
  let title: String
  let message: String

  var body: some View {
    ZStack {
      crmBackground

      VStack(spacing: 18) {
        ProgressView()
          .tint(.white)
          .scaleEffect(1.2)

        Text(title)
          .font(.system(size: 19, weight: .semibold, design: .rounded))
          .foregroundStyle(.white)

        Text(message)
          .font(.system(size: 14, weight: .medium, design: .rounded))
          .foregroundStyle(.white.opacity(0.7))
      }
      .padding(28)
      .background(crmGlassCard)
      .padding(24)
    }
  }
}

private struct CRMLoginView: View {
  @ObservedObject var store: CRMStore
  @FocusState private var focusedField: LoginField?

  private enum LoginField {
    case email
    case password
  }

  var body: some View {
    ZStack {
      crmBackground

      ScrollView {
        VStack(alignment: .leading, spacing: 22) {
          VStack(alignment: .leading, spacing: 14) {
            Text("Agustin 2.0 CRM")
              .font(.system(size: 34, weight: .bold, design: .rounded))
              .foregroundStyle(.white)

            Text("Internal lead operations app for Chicago Metal Works & Fencing. Built to receive, review, and work leads coming from the website, assistant, and Google Ads campaigns.")
              .font(.system(size: 15, weight: .medium, design: .rounded))
              .foregroundStyle(.white.opacity(0.78))
              .fixedSize(horizontal: false, vertical: true)
          }

          VStack(alignment: .leading, spacing: 16) {
            Text("Sign In")
              .font(.system(size: 20, weight: .bold, design: .rounded))
              .foregroundStyle(.white)

            VStack(alignment: .leading, spacing: 8) {
              Text("CRM email")
                .font(.system(size: 13, weight: .semibold, design: .rounded))
                .foregroundStyle(.white.opacity(0.72))

              TextField("you@company.com", text: $store.loginEmail)
                .textInputAutocapitalization(.never)
                .keyboardType(.emailAddress)
                .autocorrectionDisabled()
                .focused($focusedField, equals: .email)
                .submitLabel(.next)
                .onSubmit {
                  focusedField = .password
                }
                .crmFieldStyle()
            }

            VStack(alignment: .leading, spacing: 8) {
              Text("Password")
                .font(.system(size: 13, weight: .semibold, design: .rounded))
                .foregroundStyle(.white.opacity(0.72))

              SecureField("Enter your CRM password", text: $store.loginPassword)
                .focused($focusedField, equals: .password)
                .submitLabel(.go)
                .onSubmit {
                  Task {
                    await store.login()
                  }
                }
                .crmFieldStyle()
            }

            if !store.isConfigured {
              infoBanner(
                title: "Backend needs setup",
                body: "The backend still reports that `METALWORKS_CRM_PASSWORD` is not configured.",
                tone: .warning
              )
            }

            if !store.sessionMessage.isEmpty {
              infoBanner(
                title: "Sign-in status",
                body: store.sessionMessage,
                tone: .error
              )
            }

            Button {
              Task {
                await store.login()
              }
            } label: {
              HStack {
                if store.isLoggingIn {
                  ProgressView()
                    .tint(.black)
                }

                Text(store.isLoggingIn ? "Signing in..." : "Open Lead Inbox")
                  .font(.system(size: 16, weight: .bold, design: .rounded))
              }
              .frame(maxWidth: .infinity)
              .padding(.vertical, 16)
              .foregroundStyle(.black)
              .background(Color.white, in: RoundedRectangle(cornerRadius: 18, style: .continuous))
            }
            .disabled(store.isLoggingIn)
          }
          .padding(22)
          .background(crmGlassCard)

          VStack(alignment: .leading, spacing: 12) {
            Label("Same credentials as the web CRM", systemImage: "lock.shield")
            Label("Works best for live lead triage and follow-up", systemImage: "tray.full")
            Label("Paid-traffic leads can be filtered inside the inbox", systemImage: "megaphone")
          }
          .font(.system(size: 14, weight: .medium, design: .rounded))
          .foregroundStyle(.white.opacity(0.8))
        }
        .padding(22)
      }
    }
  }
}

private struct CRMInboxView: View {
  @ObservedObject var store: CRMStore
  @EnvironmentObject private var pushManager: PushNotificationManager

  var body: some View {
    NavigationStack {
      ZStack {
        crmBackground

        if store.dashboard == nil && store.isRefreshingDashboard {
          ScrollView {
            VStack(alignment: .leading, spacing: 18) {
              inboxHeader
              CRMLoadingCard(
                title: "Loading lead inbox...",
                message: "Pulling your latest leads, campaign tracking, and callbacks."
              )
            }
            .padding(.horizontal, 18)
            .padding(.top, 18)
            .padding(.bottom, 34)
          }
        } else {
          ScrollView {
            VStack(alignment: .leading, spacing: 18) {
              inboxHeader
              summaryStrip
              if shouldShowPushPanel {
                pushNotificationsPanel
              }
              filtersPanel

              if !store.inboxAlertMessage.isEmpty {
                infoBanner(title: "New lead", body: store.inboxAlertMessage, tone: .success)
              }

              if !store.dashboardMessage.isEmpty {
                infoBanner(title: "Inbox status", body: store.dashboardMessage, tone: .error)
              }

              if store.visibleLeads.isEmpty {
                emptyStateCard
              } else {
                VStack(spacing: 14) {
                  ForEach(store.visibleLeads) { lead in
                    NavigationLink {
                      CRMLeadDetailView(store: store, leadID: lead.id)
                    } label: {
                      LeadRowCard(
                        lead: lead,
                        isFresh: store.freshLeadIDs.contains(lead.id)
                      )
                    }
                    .buttonStyle(.plain)
                    .simultaneousGesture(
                      TapGesture().onEnded {
                        store.markLeadSeen(id: lead.id)
                      }
                    )
                  }
                }
              }

              if !(store.dashboard?.recentActivity ?? []).isEmpty {
                recentActivitySection
              }
            }
            .padding(.horizontal, 18)
            .padding(.top, 18)
            .padding(.bottom, 34)
          }
          .refreshable {
            await store.loadDashboard()
          }
        }
      }
      .navigationBarHidden(true)
      .onChange(of: store.inboxAlertToken) { _, token in
        guard token > 0 else {
          return
        }

#if canImport(UIKit)
        let generator = UINotificationFeedbackGenerator()
        generator.prepare()
        generator.notificationOccurred(.success)
#endif
      }
    }
  }

  private var shouldShowPushPanel: Bool {
    pushManager.needsPermissionPrompt ||
      pushManager.needsSystemSettings ||
      pushManager.isReadyForLiveAlerts ||
      pushManager.isSyncingDevice ||
      pushManager.isSendingTest ||
      !pushManager.statusMessage.isEmpty
  }

  private var inboxHeader: some View {
    VStack(alignment: .leading, spacing: 12) {
      HStack(alignment: .top, spacing: 12) {
        VStack(alignment: .leading, spacing: 5) {
          Text("Lead Inbox")
            .font(.system(size: 34, weight: .bold, design: .rounded))
            .foregroundStyle(.white)

          Text("Private CRM for incoming quotes, callbacks, and campaign leads.")
            .font(.system(size: 14, weight: .medium, design: .rounded))
            .foregroundStyle(.white.opacity(0.72))
        }

        Spacer()

        Button {
          Task {
            await store.logout()
          }
        } label: {
          Image(systemName: "rectangle.portrait.and.arrow.right")
            .font(.system(size: 16, weight: .bold))
            .foregroundStyle(.white)
            .padding(12)
            .background(Color.white.opacity(0.08), in: RoundedRectangle(cornerRadius: 14, style: .continuous))
        }
      }

      HStack(spacing: 10) {
        Label(store.displayName, systemImage: "person.crop.circle.fill")
          .font(.system(size: 13, weight: .semibold, design: .rounded))
          .foregroundStyle(.white.opacity(0.9))

        if !store.themeLabel.isEmpty {
          Text(store.themeLabel)
            .font(.system(size: 12, weight: .bold, design: .rounded))
            .foregroundStyle(Color(red: 0.94, green: 0.96, blue: 1.0))
            .padding(.horizontal, 10)
            .padding(.vertical, 6)
            .background(Color.white.opacity(0.1), in: Capsule())
        }

        Spacer()

        Button {
          Task {
            await store.loadDashboard()
          }
        } label: {
          Label("Refresh", systemImage: "arrow.clockwise")
            .font(.system(size: 12, weight: .bold, design: .rounded))
            .foregroundStyle(.white)
        }
      }
    }
  }

  private var summaryStrip: some View {
    let summary = store.dashboard?.summary

    return ScrollView(.horizontal, showsIndicators: false) {
      HStack(spacing: 12) {
        SummaryCard(
          title: "Total Leads",
          value: summary?.totalLeads ?? 0,
          note: "\(summary?.newLeads ?? 0) new"
        )

        SummaryCard(
          title: "Pipeline",
          value: (summary?.contactedLeads ?? 0) + (summary?.quotedLeads ?? 0) + (summary?.bookedLeads ?? 0),
          note: "\(summary?.wonLeads ?? 0) won"
        )

        SummaryCard(
          title: "Paid Traffic",
          value: store.paidLeadCount,
          note: "\(store.callbackLeadCount) with follow-up"
        )

        SummaryCard(
          title: "Quote Forms",
          value: summary?.quoteSubmits30d ?? 0,
          note: "\(summary?.phoneClicks30d ?? 0) phone clicks"
        )
      }
      .padding(.vertical, 2)
    }
  }

  private var pushNotificationsPanel: some View {
    VStack(alignment: .leading, spacing: 14) {
      Text(pushPanelTitle)
        .font(.system(size: 16, weight: .bold, design: .rounded))
        .foregroundStyle(bannerAccentColor(pushPanelTone))

      Text(pushPanelBody)
        .font(.system(size: 13, weight: .medium, design: .rounded))
        .foregroundStyle(.white.opacity(0.86))
        .fixedSize(horizontal: false, vertical: true)

      HStack(spacing: 10) {
        if pushManager.needsPermissionPrompt {
          Button {
            Task {
              await pushManager.requestAuthorization()
            }
          } label: {
            HStack(spacing: 8) {
              if pushManager.isRequestingAuthorization {
                ProgressView()
                  .tint(.black)
              }

              Text(pushManager.isRequestingAuthorization ? "Requesting..." : "Turn On Alerts")
            }
            .font(.system(size: 13, weight: .bold, design: .rounded))
            .foregroundStyle(.black)
            .padding(.horizontal, 14)
            .padding(.vertical, 12)
            .background(Color.white, in: RoundedRectangle(cornerRadius: 16, style: .continuous))
          }
          .disabled(pushManager.isRequestingAuthorization)
        } else if pushManager.needsSystemSettings {
          Button {
            openAppSettings()
          } label: {
            Text("Open Settings")
              .font(.system(size: 13, weight: .bold, design: .rounded))
              .foregroundStyle(.black)
              .padding(.horizontal, 14)
              .padding(.vertical, 12)
              .background(Color.white, in: RoundedRectangle(cornerRadius: 16, style: .continuous))
          }
        }

        if pushManager.canSendTestAlert {
          Button {
            Task {
              await pushManager.sendTestAlert()
            }
          } label: {
            HStack(spacing: 8) {
              if pushManager.isSendingTest {
                ProgressView()
                  .tint(.white)
              }

              Text(pushManager.isSendingTest ? "Sending..." : "Send Test Alert")
            }
            .font(.system(size: 13, weight: .bold, design: .rounded))
            .foregroundStyle(.white)
            .padding(.horizontal, 14)
            .padding(.vertical, 12)
            .background(Color.white.opacity(0.08), in: RoundedRectangle(cornerRadius: 16, style: .continuous))
          }
          .disabled(pushManager.isSendingTest)
        }
      }
    }
    .padding(18)
    .background(
      bannerAccentColor(pushPanelTone).opacity(0.14),
      in: RoundedRectangle(cornerRadius: 18, style: .continuous)
    )
  }

  private var pushPanelTitle: String {
    if pushManager.needsPermissionPrompt {
      return "Enable Push Alerts"
    }

    if pushManager.needsSystemSettings {
      return "Push Alerts Are Off"
    }

    if pushManager.isSyncingDevice {
      return "Registering This iPhone"
    }

    if pushManager.isReadyForLiveAlerts {
      return "Live Alerts Ready"
    }

    if pushManager.isRegisteredWithBackend && !pushManager.backendPushConfigured {
      return "Apple Push Keys Still Needed"
    }

    return "Push Alert Status"
  }

  private var pushPanelBody: String {
    if pushManager.needsPermissionPrompt {
      return "Allow notifications on this iPhone so Agustin 2.0 can alert you when a new lead or callback hits the CRM, even with the app closed."
    }

    if pushManager.needsSystemSettings {
      return "Notifications are currently off for this iPhone. Open iPhone Settings for Agustin 2.0 and turn them back on to receive live lead alerts."
    }

    if pushManager.isSyncingDevice {
      return "Apple approved notifications for this iPhone. I am linking the device with your CRM session now."
    }

    if !pushManager.statusMessage.isEmpty {
      return pushManager.statusMessage
    }

    return "This iPhone is connected for live lead alerts."
  }

  private var pushPanelTone: BannerTone {
    if pushManager.isReadyForLiveAlerts {
      return .success
    }

    return .warning
  }

  private func openAppSettings() {
#if canImport(UIKit)
    guard let url = URL(string: UIApplication.openSettingsURLString) else {
      return
    }

    UIApplication.shared.open(url)
#endif
  }

  private var filtersPanel: some View {
    VStack(alignment: .leading, spacing: 14) {
      HStack {
        Text("Filters")
          .font(.system(size: 16, weight: .bold, design: .rounded))
          .foregroundStyle(.white)

        Spacer()

        Toggle(isOn: $store.paidOnly) {
          Text("Paid traffic only")
            .font(.system(size: 13, weight: .semibold, design: .rounded))
            .foregroundStyle(.white.opacity(0.82))
        }
        .toggleStyle(.switch)
        .tint(Color(red: 0.87, green: 0.18, blue: 0.16))
      }

      HStack(spacing: 10) {
        TextField("Search name, phone, email, location...", text: $store.searchText)
          .textInputAutocapitalization(.never)
          .autocorrectionDisabled()
          .submitLabel(.search)
          .onSubmit {
            Task {
              await store.loadDashboard()
            }
          }
          .crmFieldStyle()

        Button {
          Task {
            await store.loadDashboard()
          }
        } label: {
          Image(systemName: "magnifyingglass")
            .font(.system(size: 16, weight: .bold))
            .foregroundStyle(.black)
            .padding(14)
            .background(Color.white, in: RoundedRectangle(cornerRadius: 16, style: .continuous))
        }
      }

      Menu {
        Button("All statuses") {
          store.statusFilter = ""
          Task {
            await store.loadDashboard()
          }
        }

        ForEach(store.statusOptions) { option in
          Button(option.label) {
            store.statusFilter = option.value
            Task {
              await store.loadDashboard()
            }
          }
        }
      } label: {
        HStack {
          Text(store.statusFilter.isEmpty ? "All statuses" : labelForStatus(store.statusFilter, options: store.statusOptions))
            .font(.system(size: 14, weight: .semibold, design: .rounded))
            .foregroundStyle(.white)

          Spacer()

          Image(systemName: "chevron.down")
            .foregroundStyle(.white.opacity(0.7))
        }
        .padding(.horizontal, 14)
        .padding(.vertical, 14)
        .background(Color.white.opacity(0.08), in: RoundedRectangle(cornerRadius: 18, style: .continuous))
      }
    }
    .padding(18)
    .background(crmGlassCard)
  }

  private var emptyStateCard: some View {
    VStack(alignment: .leading, spacing: 12) {
      Text("No leads match these filters")
        .font(.system(size: 18, weight: .bold, design: .rounded))
        .foregroundStyle(.white)

      Text("Try clearing the status filter or turning off the paid-traffic toggle to bring more leads back into the inbox.")
        .font(.system(size: 14, weight: .medium, design: .rounded))
        .foregroundStyle(.white.opacity(0.72))
        .fixedSize(horizontal: false, vertical: true)
    }
    .padding(20)
    .background(crmGlassCard)
  }

  private var recentActivitySection: some View {
    VStack(alignment: .leading, spacing: 14) {
      Text("Recent Activity")
        .font(.system(size: 19, weight: .bold, design: .rounded))
        .foregroundStyle(.white)

      VStack(spacing: 10) {
        ForEach(Array((store.dashboard?.recentActivity ?? []).prefix(8))) { activity in
          ActivityRow(activity: activity)
        }
      }
    }
  }
}

private struct CRMApplicantsView: View {
  @ObservedObject var store: CRMStore

  var body: some View {
    NavigationStack {
      ZStack {
        crmBackground

        if store.applicants.isEmpty && store.isRefreshingApplicants {
          ScrollView {
            VStack(alignment: .leading, spacing: 18) {
              applicantsHeader
              CRMLoadingCard(
                title: "Loading applicants...",
                message: "Pulling welders, fabricators, sales candidates, and prospectors from the hiring assistant."
              )
            }
            .padding(.horizontal, 18)
            .padding(.top, 18)
            .padding(.bottom, 34)
          }
        } else {
          ScrollView {
            VStack(alignment: .leading, spacing: 18) {
              applicantsHeader
              applicantsSummaryStrip
              applicantsFiltersPanel

              if !store.applicantsMessage.isEmpty {
                infoBanner(title: "Applicants status", body: store.applicantsMessage, tone: .error)
              }

              if store.visibleApplicants.isEmpty {
                applicantsEmptyState
              } else {
                VStack(spacing: 14) {
                  ForEach(store.visibleApplicants) { applicant in
                    NavigationLink {
                      CRMApplicantDetailView(store: store, applicantID: applicant.id)
                    } label: {
                      ApplicantRowCard(applicant: applicant)
                    }
                    .buttonStyle(.plain)
                  }
                }
              }
            }
            .padding(.horizontal, 18)
            .padding(.top, 18)
            .padding(.bottom, 34)
          }
          .refreshable {
            await store.loadApplicants()
          }
        }
      }
      .navigationBarHidden(true)
    }
  }

  private var applicantsHeader: some View {
    VStack(alignment: .leading, spacing: 12) {
      HStack(alignment: .top, spacing: 12) {
        VStack(alignment: .leading, spacing: 5) {
          Text("Applicants")
            .font(.system(size: 34, weight: .bold, design: .rounded))
            .foregroundStyle(.white)

          Text("Hiring pipeline for welders, fabricators, sales reps, and field prospectors coming through Agustin 2.0.")
            .font(.system(size: 14, weight: .medium, design: .rounded))
            .foregroundStyle(.white.opacity(0.72))
        }

        Spacer()

        Button {
          Task {
            await store.logout()
          }
        } label: {
          Image(systemName: "rectangle.portrait.and.arrow.right")
            .font(.system(size: 16, weight: .bold))
            .foregroundStyle(.white)
            .padding(12)
            .background(Color.white.opacity(0.08), in: RoundedRectangle(cornerRadius: 14, style: .continuous))
        }
      }

      HStack(spacing: 10) {
        Label(store.displayName, systemImage: "person.badge.shield.checkmark")
          .font(.system(size: 13, weight: .semibold, design: .rounded))
          .foregroundStyle(.white.opacity(0.9))

        Spacer()

        Button {
          Task {
            await store.loadApplicants()
          }
        } label: {
          Label("Refresh", systemImage: "arrow.clockwise")
            .font(.system(size: 12, weight: .bold, design: .rounded))
            .foregroundStyle(.white)
        }
      }
    }
  }

  private var applicantsSummaryStrip: some View {
    ScrollView(.horizontal, showsIndicators: false) {
      HStack(spacing: 12) {
        SummaryCard(
          title: "Applicants",
          value: store.applicants.count,
          note: "\(store.dashboard?.summary.totalApplicants ?? store.applicants.count) total in CRM"
        )

        SummaryCard(
          title: "New",
          value: store.dashboard?.summary.newApplicants ?? store.applicantNewCount,
          note: "awaiting first review"
        )

        SummaryCard(
          title: "Interviews",
          value: store.dashboard?.summary.interviewApplicants ?? store.applicantInterviewCount,
          note: "requested or scheduled"
        )

        SummaryCard(
          title: "Reachable",
          value: store.applicantWithPhoneCount,
          note: "with phone number"
        )
      }
      .padding(.vertical, 2)
    }
  }

  private var applicantsFiltersPanel: some View {
    VStack(alignment: .leading, spacing: 14) {
      Text("Filters")
        .font(.system(size: 16, weight: .bold, design: .rounded))
        .foregroundStyle(.white)

      HStack(spacing: 10) {
        TextField("Search name, role, phone, email, languages...", text: $store.applicantSearchText)
          .textInputAutocapitalization(.never)
          .autocorrectionDisabled()
          .submitLabel(.search)
          .crmFieldStyle()

        Button {
          Task {
            await store.loadApplicants()
          }
        } label: {
          Image(systemName: "magnifyingglass")
            .font(.system(size: 16, weight: .bold))
            .foregroundStyle(.black)
            .padding(14)
            .background(Color.white, in: RoundedRectangle(cornerRadius: 16, style: .continuous))
        }
      }

      HStack(spacing: 10) {
        Menu {
          ForEach(store.applicantStatusOptions) { option in
            Button(option.label) {
              store.applicantStatusFilter = option.value
            }
          }
        } label: {
          HStack {
            Text(store.applicantStatusFilter.isEmpty ? "All statuses" : labelForStatus(store.applicantStatusFilter, options: store.applicantStatusOptions))
              .font(.system(size: 14, weight: .semibold, design: .rounded))
              .foregroundStyle(.white)

            Spacer()

            Image(systemName: "chevron.down")
              .foregroundStyle(.white.opacity(0.7))
          }
          .padding(.horizontal, 14)
          .padding(.vertical, 14)
          .background(Color.white.opacity(0.08), in: RoundedRectangle(cornerRadius: 18, style: .continuous))
        }

        Menu {
          Button("All roles") {
            store.applicantRoleFilter = ""
          }

          ForEach(store.applicantRoleOptions, id: \.self) { role in
            Button(role) {
              store.applicantRoleFilter = role
            }
          }
        } label: {
          HStack {
            Text(store.applicantRoleFilter.isEmpty ? "All roles" : store.applicantRoleFilter)
              .font(.system(size: 14, weight: .semibold, design: .rounded))
              .foregroundStyle(.white)
              .lineLimit(1)

            Spacer()

            Image(systemName: "chevron.down")
              .foregroundStyle(.white.opacity(0.7))
          }
          .padding(.horizontal, 14)
          .padding(.vertical, 14)
          .background(Color.white.opacity(0.08), in: RoundedRectangle(cornerRadius: 18, style: .continuous))
        }
      }
    }
    .padding(18)
    .background(crmGlassCard)
  }

  private var applicantsEmptyState: some View {
    VStack(alignment: .leading, spacing: 12) {
      Text("No applicants match these filters")
        .font(.system(size: 18, weight: .bold, design: .rounded))
        .foregroundStyle(.white)

      Text("Try clearing the status or role filter, or wait for the hiring assistant to capture the next candidate.")
        .font(.system(size: 14, weight: .medium, design: .rounded))
        .foregroundStyle(.white.opacity(0.72))
        .fixedSize(horizontal: false, vertical: true)
    }
    .padding(20)
    .background(crmGlassCard)
  }
}

private struct CRMAgendaView: View {
  @ObservedObject var store: CRMStore

  var body: some View {
    NavigationStack {
      ZStack {
        crmBackground

        ScrollView {
          VStack(alignment: .leading, spacing: 18) {
            agendaHeader
            agendaSummaryStrip

            if !store.agendaMessage.isEmpty {
              infoBanner(title: "Agenda status", body: store.agendaMessage, tone: .error)
            }

            if store.isRefreshingAgenda && store.agendaLeads.isEmpty {
              CRMLoadingCard(
                title: "Loading agenda...",
                message: "Pulling scheduled work, booked jobs, and active projects."
              )
            } else if store.upcomingAgendaLeads.isEmpty && store.activeProjectLeads.isEmpty && store.agendaNeedsDateLeads.isEmpty {
              agendaEmptyState
            } else {
              if !store.activeProjectLeads.isEmpty {
                agendaSection(
                  title: "Working Now",
                  subtitle: "Projects dated for today or already in active production.",
                  leads: store.activeProjectLeads
                )
              }

              if !store.upcomingAgendaLeads.isEmpty {
                agendaSection(
                  title: "Upcoming Schedule",
                  subtitle: "Booked work already placed on the calendar.",
                  leads: store.upcomingAgendaLeads
                )
              }

              if !store.agendaNeedsDateLeads.isEmpty {
                agendaSection(
                  title: "Booked, Needs Date",
                  subtitle: "Sold jobs that still need a work date before they land cleanly in the agenda.",
                  leads: store.agendaNeedsDateLeads
                )
              }
            }
          }
          .padding(.horizontal, 18)
          .padding(.top, 18)
          .padding(.bottom, 34)
        }
        .refreshable {
          await store.loadAgenda()
        }
      }
      .navigationBarHidden(true)
    }
  }

  private var agendaHeader: some View {
    VStack(alignment: .leading, spacing: 12) {
      HStack(alignment: .top, spacing: 12) {
        VStack(alignment: .leading, spacing: 5) {
          Text("Agenda")
            .font(.system(size: 34, weight: .bold, design: .rounded))
            .foregroundStyle(.white)

          Text("Scheduled installs, field work, and active projects for Chicago Metal Works.")
            .font(.system(size: 14, weight: .medium, design: .rounded))
            .foregroundStyle(.white.opacity(0.72))
        }

        Spacer()

        Button {
          Task {
            await store.logout()
          }
        } label: {
          Image(systemName: "rectangle.portrait.and.arrow.right")
            .font(.system(size: 16, weight: .bold))
            .foregroundStyle(.white)
            .padding(12)
            .background(Color.white.opacity(0.08), in: RoundedRectangle(cornerRadius: 14, style: .continuous))
        }
      }

      HStack(spacing: 10) {
        Label(store.displayName, systemImage: "calendar.badge.clock")
          .font(.system(size: 13, weight: .semibold, design: .rounded))
          .foregroundStyle(.white.opacity(0.9))

        Spacer()

        Button {
          Task {
            await store.loadAgenda()
          }
        } label: {
          Label("Refresh", systemImage: "arrow.clockwise")
            .font(.system(size: 12, weight: .bold, design: .rounded))
            .foregroundStyle(.white)
        }
      }
    }
  }

  private var agendaSummaryStrip: some View {
    ScrollView(.horizontal, showsIndicators: false) {
      HStack(spacing: 12) {
        SummaryCard(
          title: "Upcoming",
          value: store.upcomingAgendaLeads.count,
          note: "scheduled jobs"
        )

        SummaryCard(
          title: "Working",
          value: store.activeProjectLeads.count,
          note: "active projects"
        )

        SummaryCard(
          title: "Need Date",
          value: store.agendaNeedsDateLeads.count,
          note: "booked with no date"
        )
      }
      .padding(.vertical, 2)
    }
  }

  private func agendaSection(title: String, subtitle: String, leads: [CRMLead]) -> some View {
    VStack(alignment: .leading, spacing: 14) {
      VStack(alignment: .leading, spacing: 4) {
        Text(title)
          .font(.system(size: 19, weight: .bold, design: .rounded))
          .foregroundStyle(.white)

        Text(subtitle)
          .font(.system(size: 13, weight: .medium, design: .rounded))
          .foregroundStyle(.white.opacity(0.72))
      }

      VStack(spacing: 14) {
        ForEach(leads) { lead in
          NavigationLink {
            CRMLeadDetailView(store: store, leadID: lead.id)
          } label: {
            AgendaProjectCard(lead: lead)
          }
          .buttonStyle(.plain)
        }
      }
    }
  }

  private var agendaEmptyState: some View {
    VStack(alignment: .leading, spacing: 12) {
      Text("No work is scheduled yet")
        .font(.system(size: 18, weight: .bold, design: .rounded))
        .foregroundStyle(.white)

      Text("Open a lead in the CRM tab, add a work date, and it will appear here under Agenda.")
        .font(.system(size: 14, weight: .medium, design: .rounded))
        .foregroundStyle(.white.opacity(0.72))
        .fixedSize(horizontal: false, vertical: true)
    }
    .padding(20)
    .background(crmGlassCard)
  }
}

private struct CRMLeadDetailView: View {
  @ObservedObject var store: CRMStore
  let leadID: String
  @Environment(\.scenePhase) private var scenePhase

  private var detail: CRMLeadDetailResponse? {
    guard store.leadDetail?.lead.id == leadID else {
      return nil
    }

    return store.leadDetail
  }

  var body: some View {
    ZStack {
      crmBackground

      ScrollView {
        VStack(alignment: .leading, spacing: 18) {
          if let detail {
            leadHero(detail.lead)
            contactSection(detail.lead)
            trackingSection(detail.lead)
            if detail.lead.hasFieldIntakeDetails {
              fieldIntakeSection(detail.lead)
            }
            jobScheduleSection(detail.lead)
            leadProfileSection(detail.lead)
            followUpSection

            if !detail.assets.isEmpty {
              photoSection(detail.assets)
            }

            if !detail.lead.conversationHistory.isEmpty {
              conversationSection(detail.lead.conversationHistory)
            }

            if !detail.activity.isEmpty {
              activitySection(detail.activity)
            }
          } else if store.isLoadingLead {
            CRMLoadingCard(title: "Loading lead...", message: "Pulling the full lead profile, notes, photos, and activity.")
          } else {
            CRMLoadingCard(title: "Lead unavailable", message: store.detailMessage.isEmpty ? "This lead could not be loaded yet." : store.detailMessage)
          }
        }
        .padding(.horizontal, 18)
        .padding(.top, 18)
        .padding(.bottom, 32)
      }
      .refreshable {
        await store.loadLeadDetail(id: leadID)
      }
    }
    .navigationTitle("Lead Detail")
    .navigationBarTitleDisplayMode(.inline)
    .toolbar {
      ToolbarItem(placement: .topBarTrailing) {
        Button {
          Task {
            await store.loadLeadDetail(id: leadID)
          }
        } label: {
          Image(systemName: "arrow.clockwise")
        }
      }
    }
    .task(id: leadID) {
      await store.loadLeadDetail(id: leadID)
    }
    .task(id: detailAutoRefreshEnabled) {
      await autoRefreshDetailLoop()
    }
  }

  private func leadHero(_ lead: CRMLead) -> some View {
    VStack(alignment: .leading, spacing: 14) {
      HStack(alignment: .top, spacing: 12) {
        VStack(alignment: .leading, spacing: 6) {
          Text(lead.displayName)
            .font(.system(size: 28, weight: .bold, design: .rounded))
            .foregroundStyle(.white)

          Text(lead.projectType.isEmpty ? "Service type still pending" : lead.projectType)
            .font(.system(size: 15, weight: .medium, design: .rounded))
            .foregroundStyle(.white.opacity(0.74))
        }

        Spacer()

        StatusBadge(status: lead.status, label: lead.statusLabel)
      }

      ScrollView(.horizontal, showsIndicators: false) {
        HStack(spacing: 10) {
          if !lead.location.isEmpty {
            LeadMetaChip(text: lead.location, systemImage: "mappin.and.ellipse")
          }

          LeadMetaChip(text: leadSourceLabel(lead), systemImage: "megaphone")

          if !lead.prospectorCaptureSummary.isEmpty {
            LeadMetaChip(text: lead.prospectorCaptureSummary, systemImage: "person.badge.shield.checkmark")
          }

          if lead.hasScheduledWorkDate {
            LeadMetaChip(text: "Work \(formatDateOnly(lead.clientDocumentWorkDate))", systemImage: "calendar")
          }
        }
      }

      if lead.isProfileIncomplete {
        infoBanner(
          title: "Lead needs more info",
          body: "Still missing: \(lead.missingFieldSummary). You can fix it below and it will sync back to the shared CRM.",
          tone: .warning
        )
      }

      if let callbackSummary = callbackSummaryText(for: lead), !callbackSummary.isEmpty {
        LeadMetaChip(text: callbackSummary, systemImage: "phone.badge.waveform")
      }

      if !lead.details.isEmpty {
        Text(lead.details)
          .font(.system(size: 15, weight: .medium, design: .rounded))
          .foregroundStyle(.white.opacity(0.82))
          .fixedSize(horizontal: false, vertical: true)
      }
    }
    .padding(22)
    .background(crmGlassCard)
  }

  private func contactSection(_ lead: CRMLead) -> some View {
    VStack(alignment: .leading, spacing: 14) {
      Text("Contact")
        .font(.system(size: 19, weight: .bold, design: .rounded))
        .foregroundStyle(.white)

      VStack(spacing: 12) {
        if !lead.phoneDigits.isEmpty {
          HStack(spacing: 10) {
            Link(destination: URL(string: "tel:+1\(lead.phoneDigits)")!) {
              ContactActionButton(title: "Call", systemImage: "phone.fill")
            }

            Link(destination: URL(string: "sms:+1\(lead.phoneDigits)")!) {
              ContactActionButton(title: "Text", systemImage: "message.fill")
            }
          }
        }

        if !lead.email.isEmpty {
          Link(destination: URL(string: "mailto:\(lead.email)")!) {
            ContactActionButton(title: lead.email, systemImage: "envelope.fill", fullWidth: true)
          }
        }

        if lead.phoneDigits.isEmpty && lead.email.isEmpty {
          Text("No direct contact info has been captured for this lead yet.")
            .font(.system(size: 14, weight: .medium, design: .rounded))
            .foregroundStyle(.white.opacity(0.72))
        }
      }
    }
  }

  private func fieldIntakeSection(_ lead: CRMLead) -> some View {
    let intakeRows = buildFieldIntakeRows(for: lead)

    return VStack(alignment: .leading, spacing: 14) {
      Text("Field Intake")
        .font(.system(size: 19, weight: .bold, design: .rounded))
        .foregroundStyle(.white)

      VStack(alignment: .leading, spacing: 10) {
        ForEach(Array(intakeRows.enumerated()), id: \.offset) { _, row in
          TrackingRow(label: row.label, value: row.value)
        }
      }
      .padding(18)
      .background(crmGlassCard)
    }
  }

  private func jobScheduleSection(_ lead: CRMLead) -> some View {
    VStack(alignment: .leading, spacing: 14) {
      Text("Job Schedule")
        .font(.system(size: 19, weight: .bold, design: .rounded))
        .foregroundStyle(.white)

      VStack(alignment: .leading, spacing: 16) {
        if lead.hasScheduledWorkDate {
          infoBanner(
            title: "Scheduled",
            body: "Current work date: \(formatDateOnly(lead.clientDocumentWorkDate)). This project is already feeding the agenda view.",
            tone: .success
          )
        } else {
          infoBanner(
            title: "Not scheduled yet",
            body: "Add a work date here and this lead will appear in the Agenda tab so operations can track it separately from the sales inbox.",
            tone: .warning
          )
        }

        Toggle(isOn: $store.workDateEnabled) {
          Text("Add work date")
            .font(.system(size: 14, weight: .semibold, design: .rounded))
            .foregroundStyle(.white.opacity(0.86))
        }
        .tint(Color(red: 0.87, green: 0.18, blue: 0.16))

        if store.workDateEnabled {
          DatePicker(
            "Work date",
            selection: $store.editableClientDocumentWorkDate,
            displayedComponents: [.date]
          )
          .datePickerStyle(.compact)
          .labelsHidden()
          .padding(.horizontal, 12)
          .padding(.vertical, 12)
          .background(Color.white.opacity(0.08), in: RoundedRectangle(cornerRadius: 16, style: .continuous))
        }

        Text("Tip: use status `Booked` for scheduled jobs and `Won` once the project is already being worked.")
          .font(.system(size: 13, weight: .medium, design: .rounded))
          .foregroundStyle(.white.opacity(0.72))
          .fixedSize(horizontal: false, vertical: true)
      }
      .padding(18)
      .background(crmGlassCard)
    }
  }

  private func trackingSection(_ lead: CRMLead) -> some View {
    VStack(alignment: .leading, spacing: 14) {
      Text("Attribution")
        .font(.system(size: 19, weight: .bold, design: .rounded))
        .foregroundStyle(.white)

      VStack(alignment: .leading, spacing: 10) {
        TrackingRow(label: "Source", value: leadSourceLabel(lead))
        TrackingRow(label: "Campaign", value: lead.tracking.campaignSummary.isEmpty ? "Not tagged" : lead.tracking.campaignSummary)
        TrackingRow(label: "Landing", value: lead.tracking.landingSummary.isEmpty ? (lead.pagePath.isEmpty ? "Unknown" : lead.pagePath) : lead.tracking.landingSummary)

        if !lead.tracking.gclid.isEmpty {
          TrackingRow(label: "GCLID", value: lead.tracking.gclid)
        }

        if !lead.pageUrl.isEmpty, let url = URL(string: lead.pageUrl) {
          Link(destination: url) {
            Label("Open source page", systemImage: "link")
              .font(.system(size: 13, weight: .semibold, design: .rounded))
              .foregroundStyle(.white)
          }
        }
      }
      .padding(18)
      .background(crmGlassCard)
    }
  }

  private func leadProfileSection(_ lead: CRMLead) -> some View {
    VStack(alignment: .leading, spacing: 14) {
      Text("Lead Profile")
        .font(.system(size: 19, weight: .bold, design: .rounded))
        .foregroundStyle(.white)

      VStack(alignment: .leading, spacing: 16) {
        if lead.isProfileIncomplete {
          infoBanner(
            title: "Sync check",
            body: "This lead came in with missing info from the website or assistant flow. Update the profile here to keep the iPhone app and web CRM aligned.",
            tone: .warning
          )
        }

        profileField(title: "Full name", placeholder: "Customer name", text: $store.editableFullName)
        profileField(title: "Phone", placeholder: "773 555 1212", text: $store.editablePhoneDisplay, keyboard: .phonePad)
        profileField(title: "Email", placeholder: "client@email.com", text: $store.editableEmail, keyboard: .emailAddress)
        profileField(title: "Service type", placeholder: "Gate repair, railing, welding...", text: $store.editableProjectType)
        profileField(title: "Job location", placeholder: "Neighborhood or city", text: $store.editableLocation)
        profileField(title: "Best contact day", placeholder: "Today, Friday, weekday mornings...", text: $store.editableBestContactDay)
        profileField(title: "Best contact time", placeholder: "2 PM, mornings, after 5...", text: $store.editableBestContactTime)

        Text("Project details")
          .font(.system(size: 13, weight: .semibold, design: .rounded))
          .foregroundStyle(.white.opacity(0.72))

        TextEditor(text: $store.editableDetails)
          .frame(minHeight: 110)
          .scrollContentBackground(.hidden)
          .padding(12)
          .background(Color.white.opacity(0.08), in: RoundedRectangle(cornerRadius: 16, style: .continuous))
          .foregroundStyle(.white)
          .font(.system(size: 15, weight: .medium, design: .rounded))
      }
      .padding(18)
      .background(crmGlassCard)
    }
  }

  private var followUpSection: some View {
    VStack(alignment: .leading, spacing: 14) {
      Text("Follow-Up")
        .font(.system(size: 19, weight: .bold, design: .rounded))
        .foregroundStyle(.white)

      VStack(alignment: .leading, spacing: 16) {
        Text("Status")
          .font(.system(size: 13, weight: .semibold, design: .rounded))
          .foregroundStyle(.white.opacity(0.72))

        Picker("Status", selection: $store.editableStatus) {
          ForEach(store.statusOptions) { option in
            Text(option.label).tag(option.value)
          }
        }
        .pickerStyle(.menu)
        .padding(.horizontal, 12)
        .padding(.vertical, 12)
        .background(Color.white.opacity(0.08), in: RoundedRectangle(cornerRadius: 16, style: .continuous))

        Text("Next action")
          .font(.system(size: 13, weight: .semibold, design: .rounded))
          .foregroundStyle(.white.opacity(0.72))

        TextField("Call back, send quote, verify photos...", text: $store.editableNextAction)
          .crmFieldStyle()

        Toggle(isOn: $store.followUpDateEnabled) {
          Text("Schedule follow-up date")
            .font(.system(size: 14, weight: .semibold, design: .rounded))
            .foregroundStyle(.white.opacity(0.86))
        }
        .tint(Color(red: 0.87, green: 0.18, blue: 0.16))

        if store.followUpDateEnabled {
          DatePicker(
            "Follow-up time",
            selection: $store.editableNextActionAt,
            displayedComponents: [.date, .hourAndMinute]
          )
          .datePickerStyle(.compact)
          .labelsHidden()
          .padding(.horizontal, 12)
          .padding(.vertical, 12)
          .background(Color.white.opacity(0.08), in: RoundedRectangle(cornerRadius: 16, style: .continuous))
        }

        Text("Private notes")
          .font(.system(size: 13, weight: .semibold, design: .rounded))
          .foregroundStyle(.white.opacity(0.72))

        TextEditor(text: $store.editablePrivateNotes)
          .frame(minHeight: 120)
          .scrollContentBackground(.hidden)
          .padding(12)
          .background(Color.white.opacity(0.08), in: RoundedRectangle(cornerRadius: 16, style: .continuous))
          .foregroundStyle(.white)
          .font(.system(size: 15, weight: .medium, design: .rounded))

        Text("Activity note")
          .font(.system(size: 13, weight: .semibold, design: .rounded))
          .foregroundStyle(.white.opacity(0.72))

        TextField("Optional note for the activity log", text: $store.editableNote)
          .crmFieldStyle()

        if !store.saveFeedback.isEmpty {
          infoBanner(title: "Lead update", body: store.saveFeedback, tone: store.saveFeedback == "Lead saved." ? .success : .error)
        }

        Button {
          Task {
            await store.saveLead()
          }
        } label: {
          HStack {
            if store.isSavingLead {
              ProgressView()
                .tint(.black)
            }

            Text(store.isSavingLead ? "Saving..." : "Save Lead")
              .font(.system(size: 16, weight: .bold, design: .rounded))
          }
          .frame(maxWidth: .infinity)
          .padding(.vertical, 16)
          .foregroundStyle(.black)
          .background(Color.white, in: RoundedRectangle(cornerRadius: 18, style: .continuous))
        }
        .disabled(store.isSavingLead)
      }
      .padding(18)
      .background(crmGlassCard)
    }
  }

  private var detailAutoRefreshEnabled: Bool {
    scenePhase == .active && detail != nil && !hasUnsavedLocalEdits
  }

  private func autoRefreshDetailLoop() async {
    guard detailAutoRefreshEnabled else {
      return
    }

    while !Task.isCancelled {
      do {
        try await Task.sleep(for: .seconds(AppConfig.leadDetailAutoRefreshInterval))
      } catch {
        return
      }

      guard detailAutoRefreshEnabled else {
        return
      }

      await store.loadLeadDetail(id: leadID)
    }
  }

  private var hasUnsavedLocalEdits: Bool {
    guard let lead = detail?.lead else {
      return false
    }

    return normalized(store.editableFullName) != normalized(lead.fullName) ||
      normalized(store.editablePhoneDisplay) != normalized(lead.phoneDisplay) ||
      normalized(store.editableEmail) != normalized(lead.email) ||
      normalized(store.editableProjectType) != normalized(lead.projectType) ||
      normalized(store.editableLocation) != normalized(lead.location) ||
      normalized(store.editableDetails) != normalized(lead.details) ||
      normalized(store.editableBestContactDay) != normalized(lead.bestContactDay) ||
      normalized(store.editableBestContactTime) != normalized(lead.bestContactTime) ||
      workDateSyncString != normalizedDateOnly(lead.clientDocumentWorkDate) ||
      normalized(store.editableStatus) != normalized(lead.status) ||
      normalized(store.editableNextAction) != normalized(lead.nextAction) ||
      normalized(store.editablePrivateNotes) != normalized(lead.privateNotes) ||
      dateSyncString != lead.nextActionAt
  }

  private var dateSyncString: String {
    store.followUpDateEnabled ? AppDateFormatting.apiDateTime.string(from: store.editableNextActionAt) : ""
  }

  private var workDateSyncString: String {
    store.workDateEnabled ? AppDateFormatting.apiDateOnly.string(from: store.editableClientDocumentWorkDate) : ""
  }

  private func normalized(_ value: String) -> String {
    value.trimmingCharacters(in: .whitespacesAndNewlines)
  }

  private func profileField(
    title: String,
    placeholder: String,
    text: Binding<String>,
    keyboard: UIKeyboardType = .default
  ) -> some View {
    VStack(alignment: .leading, spacing: 8) {
      Text(title)
        .font(.system(size: 13, weight: .semibold, design: .rounded))
        .foregroundStyle(.white.opacity(0.72))

      TextField(placeholder, text: text)
        .textInputAutocapitalization(keyboard == .emailAddress ? .never : .sentences)
        .keyboardType(keyboard)
        .autocorrectionDisabled(keyboard == .emailAddress)
        .crmFieldStyle()
    }
  }

  private func photoSection(_ assets: [CRMLeadAsset]) -> some View {
    VStack(alignment: .leading, spacing: 14) {
      Text("Photos")
        .font(.system(size: 19, weight: .bold, design: .rounded))
        .foregroundStyle(.white)

      ScrollView(.horizontal, showsIndicators: false) {
        HStack(spacing: 12) {
          ForEach(assets) { asset in
            VStack(alignment: .leading, spacing: 8) {
              if let url = store.assetURL(for: asset) {
                AsyncImage(url: url) { phase in
                  switch phase {
                  case .success(let image):
                    image
                      .resizable()
                      .scaledToFill()
                  case .failure:
                    photoPlaceholder
                  case .empty:
                    ZStack {
                      RoundedRectangle(cornerRadius: 18, style: .continuous)
                        .fill(Color.white.opacity(0.08))
                      ProgressView()
                        .tint(.white)
                    }
                  @unknown default:
                    photoPlaceholder
                  }
                }
                .frame(width: 190, height: 150)
                .clipShape(RoundedRectangle(cornerRadius: 18, style: .continuous))
              } else {
                photoPlaceholder
              }

              Text(asset.fileName)
                .font(.system(size: 12, weight: .semibold, design: .rounded))
                .foregroundStyle(.white.opacity(0.78))
                .lineLimit(2)
            }
          }
        }
      }
    }
  }

  private var photoPlaceholder: some View {
    ZStack {
      RoundedRectangle(cornerRadius: 18, style: .continuous)
        .fill(Color.white.opacity(0.08))

      Image(systemName: "photo")
        .font(.system(size: 24, weight: .semibold))
        .foregroundStyle(.white.opacity(0.45))
    }
    .frame(width: 190, height: 150)
  }

  private func conversationSection(_ entries: [CRMConversationEntry]) -> some View {
    VStack(alignment: .leading, spacing: 14) {
      Text("Conversation")
        .font(.system(size: 19, weight: .bold, design: .rounded))
        .foregroundStyle(.white)

      VStack(spacing: 10) {
        ForEach(entries.suffix(8)) { entry in
          HStack {
            if entry.role == "assistant" {
              Spacer(minLength: 50)
            }

            VStack(alignment: .leading, spacing: 6) {
              Text(entry.role == "assistant" ? "ASSISTANT" : "VISITOR")
                .font(.system(size: 11, weight: .bold, design: .rounded))
                .foregroundStyle(entry.role == "assistant" ? Color.black.opacity(0.58) : Color.white.opacity(0.62))

              Text(entry.content)
                .font(.system(size: 15, weight: .medium, design: .rounded))
                .foregroundStyle(entry.role == "assistant" ? Color.black.opacity(0.82) : .white)
                .fixedSize(horizontal: false, vertical: true)
            }
            .padding(.horizontal, 16)
            .padding(.vertical, 14)
            .background(
              entry.role == "assistant"
                ? AnyShapeStyle(Color.white)
                : AnyShapeStyle(Color.white.opacity(0.08))
            )
            .clipShape(RoundedRectangle(cornerRadius: 20, style: .continuous))

            if entry.role != "assistant" {
              Spacer(minLength: 50)
            }
          }
        }
      }
    }
  }

  private func activitySection(_ activity: [CRMLeadActivity]) -> some View {
    VStack(alignment: .leading, spacing: 14) {
      Text("Activity")
        .font(.system(size: 19, weight: .bold, design: .rounded))
        .foregroundStyle(.white)

      VStack(spacing: 10) {
        ForEach(Array(activity.prefix(12))) { item in
          ActivityRow(activity: item)
        }
      }
    }
  }
}

private struct CRMApplicantDetailView: View {
  @ObservedObject var store: CRMStore
  let applicantID: String
  @Environment(\.scenePhase) private var scenePhase

  private var detail: CRMApplicantDetailResponse? {
    guard store.applicantDetail?.applicant.id == applicantID else {
      return nil
    }

    return store.applicantDetail
  }

  var body: some View {
    ZStack {
      crmBackground

      ScrollView {
        VStack(alignment: .leading, spacing: 18) {
          if let detail {
            applicantHero(detail.applicant)
            applicantContactSection(detail.applicant)
            applicantProfileSection(detail.applicant)
            applicantFollowUpSection(detail.applicant)
            applicantSourceSection(detail.applicant)

            if !detail.applicant.conversationHistory.isEmpty {
              applicantConversationSection(detail.applicant.conversationHistory)
            }

            if !detail.activity.isEmpty {
              applicantActivitySection(detail.activity)
            }
          } else if store.isLoadingApplicant {
            CRMLoadingCard(title: "Loading applicant...", message: "Pulling the hiring profile, conversation, and activity log.")
          } else {
            CRMLoadingCard(title: "Applicant unavailable", message: store.applicantDetailMessage.isEmpty ? "This applicant could not be loaded yet." : store.applicantDetailMessage)
          }
        }
        .padding(.horizontal, 18)
        .padding(.top, 18)
        .padding(.bottom, 32)
      }
      .refreshable {
        await store.loadApplicantDetail(id: applicantID)
      }
    }
    .navigationTitle("Applicant")
    .navigationBarTitleDisplayMode(.inline)
    .toolbar {
      ToolbarItem(placement: .topBarTrailing) {
        Button {
          Task {
            await store.loadApplicantDetail(id: applicantID)
          }
        } label: {
          Image(systemName: "arrow.clockwise")
        }
      }
    }
    .task(id: applicantID) {
      await store.loadApplicantDetail(id: applicantID)
    }
    .task(id: detailAutoRefreshEnabled) {
      await autoRefreshApplicantDetailLoop()
    }
  }

  private func applicantHero(_ applicant: CRMApplicant) -> some View {
    VStack(alignment: .leading, spacing: 14) {
      HStack(alignment: .top, spacing: 12) {
        VStack(alignment: .leading, spacing: 6) {
          Text(applicant.displayName)
            .font(.system(size: 28, weight: .bold, design: .rounded))
            .foregroundStyle(.white)

          Text(applicant.roleLabel)
            .font(.system(size: 15, weight: .medium, design: .rounded))
            .foregroundStyle(.white.opacity(0.74))
        }

        Spacer()

        StatusBadge(status: applicant.status, label: applicant.statusLabel)
      }

      ScrollView(.horizontal, showsIndicators: false) {
        HStack(spacing: 10) {
          LeadMetaChip(text: applicantSourceLabel(applicant), systemImage: "person.text.rectangle")

          if !applicant.location.isEmpty {
            LeadMetaChip(text: applicant.location, systemImage: "mappin.and.ellipse")
          }

          ForEach(applicant.profileHighlights, id: \.self) { item in
            LeadMetaChip(text: item, systemImage: "sparkles")
          }
        }
      }

      if !applicant.detailsSummary.isEmpty {
        Text(applicant.detailsSummary)
          .font(.system(size: 15, weight: .medium, design: .rounded))
          .foregroundStyle(.white.opacity(0.82))
          .fixedSize(horizontal: false, vertical: true)
      } else if !applicant.experienceSummary.isEmpty {
        Text(applicant.experienceSummary)
          .font(.system(size: 15, weight: .medium, design: .rounded))
          .foregroundStyle(.white.opacity(0.82))
          .fixedSize(horizontal: false, vertical: true)
      }
    }
    .padding(22)
    .background(crmGlassCard)
  }

  private func applicantContactSection(_ applicant: CRMApplicant) -> some View {
    VStack(alignment: .leading, spacing: 14) {
      Text("Contact")
        .font(.system(size: 19, weight: .bold, design: .rounded))
        .foregroundStyle(.white)

      VStack(spacing: 12) {
        if !applicant.phoneDigits.isEmpty {
          HStack(spacing: 10) {
            Link(destination: URL(string: "tel:+1\(applicant.phoneDigits)")!) {
              ContactActionButton(title: "Call", systemImage: "phone.fill")
            }

            Link(destination: URL(string: "sms:+1\(applicant.phoneDigits)")!) {
              ContactActionButton(title: "Text", systemImage: "message.fill")
            }
          }
        }

        if !applicant.email.isEmpty {
          Link(destination: URL(string: "mailto:\(applicant.email)")!) {
            ContactActionButton(title: applicant.email, systemImage: "envelope.fill", fullWidth: true)
          }
        }

        if applicant.phoneDigits.isEmpty && applicant.email.isEmpty {
          Text("No direct contact info has been captured for this applicant yet.")
            .font(.system(size: 14, weight: .medium, design: .rounded))
            .foregroundStyle(.white.opacity(0.72))
        }
      }
    }
  }

  private func applicantProfileSection(_ applicant: CRMApplicant) -> some View {
    return VStack(alignment: .leading, spacing: 14) {
      Text("Applicant Profile")
        .font(.system(size: 19, weight: .bold, design: .rounded))
        .foregroundStyle(.white)

      VStack(alignment: .leading, spacing: 16) {
        if applicant.sanitizedFullName.isEmpty || applicant.phoneDigits.isEmpty || applicant.positionApplied.isEmpty {
          infoBanner(
            title: "Applicant needs cleanup",
            body: "Edit the missing profile fields here so hiring and the web CRM stay aligned.",
            tone: .warning
          )
        }

        applicantProfileField(title: "Full name", placeholder: "Applicant name", text: $store.editableApplicantFullName)
        applicantProfileField(title: "Phone", placeholder: "773 555 1212", text: $store.editableApplicantPhoneDisplay, keyboard: .phonePad)
        applicantProfileField(title: "Email", placeholder: "applicant@email.com", text: $store.editableApplicantEmail, keyboard: .emailAddress)
        applicantProfileField(title: "Position applied", placeholder: "Welder, fabricator, sales...", text: $store.editableApplicantPositionApplied)
        applicantProfileField(title: "Languages", placeholder: "English, Spanish, bilingual...", text: $store.editableApplicantLanguages)
        applicantProfileField(title: "Years experience", placeholder: "5", text: $store.editableApplicantYearsExperience, keyboard: .numbersAndPunctuation)
        applicantProfileField(title: "Location", placeholder: "Chicago, Albany Park...", text: $store.editableApplicantLocation)
        applicantProfileField(title: "Preferred interview day", placeholder: "Tomorrow, Friday, weekday mornings...", text: $store.editableApplicantBestInterviewDay)
        applicantProfileField(title: "Preferred interview time", placeholder: "2 PM, after work, mornings...", text: $store.editableApplicantBestInterviewTime)

        applicantYesNoPicker(title: "Own tools", selection: $store.editableApplicantHasTools)
        applicantYesNoPicker(title: "Transportation", selection: $store.editableApplicantHasTransportation)
        applicantYesNoPicker(title: "Field ready", selection: $store.editableApplicantFieldReady)

        Text("Experience summary")
          .font(.system(size: 13, weight: .semibold, design: .rounded))
          .foregroundStyle(.white.opacity(0.72))

        TextEditor(text: $store.editableApplicantExperienceSummary)
          .frame(minHeight: 120)
          .scrollContentBackground(.hidden)
          .padding(12)
          .background(Color.white.opacity(0.08), in: RoundedRectangle(cornerRadius: 16, style: .continuous))
          .foregroundStyle(.white)
          .font(.system(size: 15, weight: .medium, design: .rounded))

        if !applicant.lastUserMessage.isEmpty {
          TrackingRow(label: "Last applicant message", value: applicant.lastUserMessage)
        }

        if !applicant.detailsSummary.isEmpty {
          TrackingRow(label: "Assistant summary", value: applicant.detailsSummary)
        }
      }
      .padding(18)
      .background(crmGlassCard)
    }
  }

  private func applicantFollowUpSection(_ applicant: CRMApplicant) -> some View {
    VStack(alignment: .leading, spacing: 14) {
      Text("Hiring Follow-Up")
        .font(.system(size: 19, weight: .bold, design: .rounded))
        .foregroundStyle(.white)

      VStack(alignment: .leading, spacing: 16) {
        if !applicant.interviewRequestedAt.isEmpty || !applicant.nextActionAt.isEmpty {
          infoBanner(
            title: "Interview signal captured",
            body: applicant.nextActionAt.isEmpty
              ? "This applicant already requested an interview. You can tighten the status, schedule, and notes below."
              : "Current follow-up is set for \(formatDateTime(applicant.nextActionAt)). You can reschedule it here.",
            tone: .success
          )
        }

        Text("Status")
          .font(.system(size: 13, weight: .semibold, design: .rounded))
          .foregroundStyle(.white.opacity(0.72))

        Picker("Status", selection: $store.editableApplicantStatus) {
          ForEach(store.applicantStatusOptions.filter { !$0.value.isEmpty }) { option in
            Text(option.label).tag(option.value)
          }
        }
        .pickerStyle(.menu)
        .padding(.horizontal, 12)
        .padding(.vertical, 12)
        .background(Color.white.opacity(0.08), in: RoundedRectangle(cornerRadius: 16, style: .continuous))

        Text("Next action")
          .font(.system(size: 13, weight: .semibold, design: .rounded))
          .foregroundStyle(.white.opacity(0.72))

        TextField("Call, schedule phone interview, send details...", text: $store.editableApplicantNextAction)
          .crmFieldStyle()

        Toggle(isOn: $store.applicantFollowUpDateEnabled) {
          Text("Schedule follow-up date")
            .font(.system(size: 14, weight: .semibold, design: .rounded))
            .foregroundStyle(.white.opacity(0.86))
        }
        .tint(Color(red: 0.87, green: 0.18, blue: 0.16))

        if store.applicantFollowUpDateEnabled {
          DatePicker(
            "Follow-up time",
            selection: $store.editableApplicantNextActionAt,
            displayedComponents: [.date, .hourAndMinute]
          )
          .datePickerStyle(.compact)
          .labelsHidden()
          .padding(.horizontal, 12)
          .padding(.vertical, 12)
          .background(Color.white.opacity(0.08), in: RoundedRectangle(cornerRadius: 16, style: .continuous))
        }

        Text("Private notes")
          .font(.system(size: 13, weight: .semibold, design: .rounded))
          .foregroundStyle(.white.opacity(0.72))

        TextEditor(text: $store.editableApplicantPrivateNotes)
          .frame(minHeight: 120)
          .scrollContentBackground(.hidden)
          .padding(12)
          .background(Color.white.opacity(0.08), in: RoundedRectangle(cornerRadius: 16, style: .continuous))
          .foregroundStyle(.white)
          .font(.system(size: 15, weight: .medium, design: .rounded))

        Text("Activity note")
          .font(.system(size: 13, weight: .semibold, design: .rounded))
          .foregroundStyle(.white.opacity(0.72))

        TextField("Optional note for the activity log", text: $store.editableApplicantNote)
          .crmFieldStyle()

        if !store.applicantSaveFeedback.isEmpty {
          infoBanner(
            title: "Applicant update",
            body: store.applicantSaveFeedback,
            tone: store.applicantSaveFeedback == "Applicant saved." ? .success : .error
          )
        }

        Button {
          Task {
            await store.saveApplicant()
          }
        } label: {
          HStack {
            if store.isSavingApplicant {
              ProgressView()
                .tint(.black)
            }

            Text(store.isSavingApplicant ? "Saving..." : "Save Applicant")
              .font(.system(size: 16, weight: .bold, design: .rounded))
          }
          .frame(maxWidth: .infinity)
          .padding(.vertical, 16)
          .foregroundStyle(.black)
          .background(Color.white, in: RoundedRectangle(cornerRadius: 18, style: .continuous))
        }
        .disabled(store.isSavingApplicant)
      }
      .padding(18)
      .background(crmGlassCard)
    }
  }

  private func applicantSourceSection(_ applicant: CRMApplicant) -> some View {
    VStack(alignment: .leading, spacing: 14) {
      Text("Source")
        .font(.system(size: 19, weight: .bold, design: .rounded))
        .foregroundStyle(.white)

      VStack(alignment: .leading, spacing: 10) {
        TrackingRow(label: "Channel", value: applicantSourceLabel(applicant))
        TrackingRow(label: "Page", value: applicant.pagePath.isEmpty ? (applicant.pageUrl.isEmpty ? "Unknown" : applicant.pageUrl) : applicant.pagePath)

        if !applicant.tracking.campaignSummary.isEmpty {
          TrackingRow(label: "Campaign", value: applicant.tracking.campaignSummary)
        }

        if !applicant.pageUrl.isEmpty, let url = URL(string: applicant.pageUrl) {
          Link(destination: url) {
            Label("Open source page", systemImage: "link")
              .font(.system(size: 13, weight: .semibold, design: .rounded))
              .foregroundStyle(.white)
          }
        }
      }
      .padding(18)
      .background(crmGlassCard)
    }
  }

  private func applicantConversationSection(_ entries: [CRMConversationEntry]) -> some View {
    VStack(alignment: .leading, spacing: 14) {
      Text("Conversation")
        .font(.system(size: 19, weight: .bold, design: .rounded))
        .foregroundStyle(.white)

      VStack(spacing: 10) {
        ForEach(entries.suffix(8)) { entry in
          HStack {
            if entry.role == "assistant" {
              Spacer(minLength: 50)
            }

            VStack(alignment: .leading, spacing: 6) {
              Text(entry.role == "assistant" ? "ASSISTANT" : "APPLICANT")
                .font(.system(size: 11, weight: .bold, design: .rounded))
                .foregroundStyle(entry.role == "assistant" ? Color.black.opacity(0.58) : Color.white.opacity(0.62))

              Text(entry.content)
                .font(.system(size: 15, weight: .medium, design: .rounded))
                .foregroundStyle(entry.role == "assistant" ? Color.black.opacity(0.82) : .white)
                .fixedSize(horizontal: false, vertical: true)
            }
            .padding(.horizontal, 16)
            .padding(.vertical, 14)
            .background(
              entry.role == "assistant"
                ? AnyShapeStyle(Color.white)
                : AnyShapeStyle(Color.white.opacity(0.08))
            )
            .clipShape(RoundedRectangle(cornerRadius: 20, style: .continuous))

            if entry.role != "assistant" {
              Spacer(minLength: 50)
            }
          }
        }
      }
    }
  }

  private func applicantActivitySection(_ activity: [CRMLeadActivity]) -> some View {
    VStack(alignment: .leading, spacing: 14) {
      Text("Activity")
        .font(.system(size: 19, weight: .bold, design: .rounded))
        .foregroundStyle(.white)

      VStack(spacing: 10) {
        ForEach(Array(activity.prefix(12))) { item in
          ActivityRow(activity: item)
        }
      }
    }
  }

  private var detailAutoRefreshEnabled: Bool {
    scenePhase == .active && detail != nil && !hasUnsavedLocalEdits
  }

  private func autoRefreshApplicantDetailLoop() async {
    guard detailAutoRefreshEnabled else {
      return
    }

    while !Task.isCancelled {
      do {
        try await Task.sleep(for: .seconds(AppConfig.leadDetailAutoRefreshInterval))
      } catch {
        return
      }

      guard detailAutoRefreshEnabled else {
        return
      }

      await store.loadApplicantDetail(id: applicantID)
    }
  }

  private var hasUnsavedLocalEdits: Bool {
    guard let applicant = detail?.applicant else {
      return false
    }

    return normalized(store.editableApplicantFullName) != normalized(applicant.sanitizedFullName) ||
      normalized(store.editableApplicantPhoneDisplay) != normalized(applicant.phoneDisplay) ||
      normalized(store.editableApplicantEmail) != normalized(applicant.email) ||
      normalized(store.editableApplicantPositionApplied) != normalized(applicant.positionApplied) ||
      normalized(store.editableApplicantLanguages) != normalized(applicant.languages) ||
      normalized(store.editableApplicantYearsExperience) != normalized(applicant.yearsExperience) ||
      normalized(store.editableApplicantExperienceSummary) != normalized(applicant.experienceSummary) ||
      normalized(store.editableApplicantHasTools) != normalized(applicant.hasTools) ||
      normalized(store.editableApplicantHasTransportation) != normalized(applicant.hasTransportation) ||
      normalized(store.editableApplicantFieldReady) != normalized(applicant.fieldReady) ||
      normalized(store.editableApplicantLocation) != normalized(applicant.location) ||
      normalized(store.editableApplicantBestInterviewDay) != normalized(applicant.bestInterviewDay) ||
      normalized(store.editableApplicantBestInterviewTime) != normalized(applicant.bestInterviewTime) ||
      normalized(store.editableApplicantStatus) != normalized(applicant.status) ||
      normalized(store.editableApplicantNextAction) != normalized(applicant.nextAction) ||
      normalized(store.editableApplicantPrivateNotes) != normalized(applicant.manualPrivateNotes) ||
      applicantDateSyncString != applicant.nextActionAt
  }

  private var applicantDateSyncString: String {
    store.applicantFollowUpDateEnabled ? AppDateFormatting.apiDateTime.string(from: store.editableApplicantNextActionAt) : ""
  }

  private func normalized(_ value: String) -> String {
    value.trimmingCharacters(in: .whitespacesAndNewlines)
  }

  private func applicantProfileField(
    title: String,
    placeholder: String,
    text: Binding<String>,
    keyboard: UIKeyboardType = .default
  ) -> some View {
    VStack(alignment: .leading, spacing: 8) {
      Text(title)
        .font(.system(size: 13, weight: .semibold, design: .rounded))
        .foregroundStyle(.white.opacity(0.72))

      TextField(placeholder, text: text)
        .textInputAutocapitalization(keyboard == .emailAddress ? .never : .sentences)
        .keyboardType(keyboard)
        .autocorrectionDisabled(keyboard == .emailAddress)
        .crmFieldStyle()
    }
  }

  private func applicantYesNoPicker(title: String, selection: Binding<String>) -> some View {
    VStack(alignment: .leading, spacing: 8) {
      Text(title)
        .font(.system(size: 13, weight: .semibold, design: .rounded))
        .foregroundStyle(.white.opacity(0.72))

      Picker(title, selection: selection) {
        Text("Pending").tag("")
        Text("Yes").tag("yes")
        Text("No").tag("no")
      }
      .pickerStyle(.segmented)
    }
  }
}

private struct SummaryCard: View {
  let title: String
  let value: Int
  let note: String

  var body: some View {
    VStack(alignment: .leading, spacing: 10) {
      Text(title.uppercased())
        .font(.system(size: 11, weight: .bold, design: .rounded))
        .foregroundStyle(.white.opacity(0.62))

      Text("\(value)")
        .font(.system(size: 30, weight: .bold, design: .rounded))
        .foregroundStyle(.white)

      Text(note)
        .font(.system(size: 13, weight: .medium, design: .rounded))
        .foregroundStyle(.white.opacity(0.72))
    }
    .frame(width: 170, alignment: .leading)
    .padding(18)
    .background(crmGlassCard)
  }
}

private struct ApplicantRowCard: View {
  let applicant: CRMApplicant

  var body: some View {
    VStack(alignment: .leading, spacing: 14) {
      HStack(alignment: .top, spacing: 12) {
        VStack(alignment: .leading, spacing: 5) {
          Text(applicant.displayName)
            .font(.system(size: 18, weight: .bold, design: .rounded))
            .foregroundStyle(.white)

          Text(applicant.roleLabel)
            .font(.system(size: 14, weight: .medium, design: .rounded))
            .foregroundStyle(.white.opacity(0.72))
        }

        Spacer()

        StatusBadge(status: applicant.status, label: applicant.statusLabel)
      }

      if !applicant.location.isEmpty {
        Text(applicant.location)
          .font(.system(size: 13, weight: .medium, design: .rounded))
          .foregroundStyle(.white.opacity(0.78))
      }

      HStack(spacing: 10) {
        LeadMetaChip(text: applicantSourceLabel(applicant), systemImage: "person.text.rectangle")

        if !applicant.languages.isEmpty {
          LeadMetaChip(text: applicant.languages, systemImage: "globe")
        }

        if !applicant.yearsExperience.isEmpty {
          LeadMetaChip(text: "\(applicant.yearsExperience) yrs", systemImage: "hammer.fill")
        }
      }

      let summary = applicant.detailsSummary.isEmpty ? applicant.experienceSummary : applicant.detailsSummary
      if !summary.isEmpty {
        Text(summary)
          .font(.system(size: 13, weight: .medium, design: .rounded))
          .foregroundStyle(.white.opacity(0.74))
          .lineLimit(3)
      }

      HStack {
        Text(applicant.phoneDisplay.isEmpty ? (applicant.email.isEmpty ? applicant.roleLabel : applicant.email) : applicant.phoneDisplay)
          .font(.system(size: 12, weight: .semibold, design: .rounded))
          .foregroundStyle(.white.opacity(0.82))

        Spacer()

        Text(applicant.lastContactAt.isEmpty ? formatDateTime(applicant.updatedAt) : formatDateTime(applicant.lastContactAt))
          .font(.system(size: 12, weight: .medium, design: .rounded))
          .foregroundStyle(.white.opacity(0.62))
      }
    }
    .padding(18)
    .background(crmGlassCard)
  }
}

private struct LeadRowCard: View {
  let lead: CRMLead
  var isFresh = false

  var body: some View {
    VStack(alignment: .leading, spacing: 14) {
      HStack(alignment: .top, spacing: 12) {
        VStack(alignment: .leading, spacing: 5) {
          Text(lead.displayName)
            .font(.system(size: 18, weight: .bold, design: .rounded))
            .foregroundStyle(.white)

          Text(lead.projectType.isEmpty ? "Service pending" : lead.projectType)
            .font(.system(size: 14, weight: .medium, design: .rounded))
            .foregroundStyle(.white.opacity(0.72))
        }

        Spacer()

        HStack(spacing: 8) {
          if isFresh {
            Text("NEW")
              .font(.system(size: 11, weight: .bold, design: .rounded))
              .foregroundStyle(Color(red: 0.06, green: 0.07, blue: 0.09))
              .padding(.horizontal, 10)
              .padding(.vertical, 6)
              .background(Color(red: 1.0, green: 0.88, blue: 0.44), in: Capsule())
          }

          StatusBadge(status: lead.status, label: lead.statusLabel)
        }
      }

      if !lead.location.isEmpty {
        Text(lead.location)
          .font(.system(size: 13, weight: .medium, design: .rounded))
          .foregroundStyle(.white.opacity(0.78))
      }

      if !lead.prospectorCaptureSummary.isEmpty {
        Text(lead.prospectorCaptureSummary)
          .font(.system(size: 12, weight: .semibold, design: .rounded))
          .foregroundStyle(.white.opacity(0.7))
      }

      HStack(spacing: 10) {
        LeadMetaChip(text: leadSourceLabel(lead), systemImage: "megaphone")

        if let callbackSummary = callbackSummaryText(for: lead), !callbackSummary.isEmpty {
          LeadMetaChip(text: callbackSummary, systemImage: "phone.badge.waveform")
        }
      }

      if lead.isProfileIncomplete {
        LeadMetaChip(text: "Needs \(lead.missingFieldSummary)", systemImage: "exclamationmark.bubble")
      }

      HStack {
        Text(lead.lastContactAt.isEmpty ? formatDateTime(lead.updatedAt) : formatDateTime(lead.lastContactAt))
          .font(.system(size: 12, weight: .medium, design: .rounded))
          .foregroundStyle(.white.opacity(0.62))

        Spacer()

        if !lead.phoneDisplay.isEmpty {
          Text(lead.phoneDisplay)
            .font(.system(size: 12, weight: .semibold, design: .rounded))
            .foregroundStyle(.white.opacity(0.82))
        }
      }
    }
    .padding(18)
    .background {
      RoundedRectangle(cornerRadius: 24, style: .continuous)
        .fill(
          isFresh
            ? AnyShapeStyle(
                LinearGradient(
                  colors: [
                    Color(red: 1.0, green: 0.88, blue: 0.44).opacity(0.22),
                    Color.white.opacity(0.08),
                  ],
                  startPoint: .topLeading,
                  endPoint: .bottomTrailing
                )
              )
            : AnyShapeStyle(crmGlassCard)
        )
    }
    .overlay(
      RoundedRectangle(cornerRadius: 24, style: .continuous)
        .stroke(
          isFresh ? Color(red: 1.0, green: 0.88, blue: 0.44).opacity(0.55) : Color.clear,
          lineWidth: 1
        )
    )
  }
}

private struct AgendaProjectCard: View {
  let lead: CRMLead

  var body: some View {
    VStack(alignment: .leading, spacing: 14) {
      HStack(alignment: .top, spacing: 12) {
        VStack(alignment: .leading, spacing: 5) {
          Text(lead.displayName)
            .font(.system(size: 18, weight: .bold, design: .rounded))
            .foregroundStyle(.white)

          Text(lead.projectType.isEmpty ? "Service pending" : lead.projectType)
            .font(.system(size: 14, weight: .medium, design: .rounded))
            .foregroundStyle(.white.opacity(0.72))
        }

        Spacer()

        StatusBadge(status: lead.status, label: lead.statusLabel)
      }

      if !lead.location.isEmpty {
        Text(lead.location)
          .font(.system(size: 13, weight: .medium, design: .rounded))
          .foregroundStyle(.white.opacity(0.78))
      }

      HStack(spacing: 10) {
        if lead.hasScheduledWorkDate {
          LeadMetaChip(text: formatDateOnly(lead.clientDocumentWorkDate), systemImage: "calendar")
        } else {
          LeadMetaChip(text: "Needs work date", systemImage: "calendar.badge.exclamationmark")
        }

        if !lead.prospectorCaptureSummary.isEmpty {
          LeadMetaChip(text: lead.prospectorCaptureSummary, systemImage: "person.badge.shield.checkmark")
        }
      }

      if !lead.clientDocumentDescription.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
        Text(lead.clientDocumentDescription)
          .font(.system(size: 13, weight: .medium, design: .rounded))
          .foregroundStyle(.white.opacity(0.74))
          .lineLimit(3)
      } else if !lead.details.isEmpty {
        Text(lead.details)
          .font(.system(size: 13, weight: .medium, design: .rounded))
          .foregroundStyle(.white.opacity(0.74))
          .lineLimit(3)
      }

      HStack {
        Text(lead.phoneDisplay.isEmpty ? (lead.email.isEmpty ? leadSourceLabel(lead) : lead.email) : lead.phoneDisplay)
          .font(.system(size: 12, weight: .semibold, design: .rounded))
          .foregroundStyle(.white.opacity(0.82))

        Spacer()

        Text(lead.lastContactAt.isEmpty ? formatDateTime(lead.updatedAt) : formatDateTime(lead.lastContactAt))
          .font(.system(size: 12, weight: .medium, design: .rounded))
          .foregroundStyle(.white.opacity(0.62))
      }
    }
    .padding(18)
    .background(crmGlassCard)
  }
}

private struct StatusBadge: View {
  let status: String
  let label: String

  var body: some View {
    Text(label.isEmpty ? status.capitalized : label)
      .font(.system(size: 12, weight: .bold, design: .rounded))
      .foregroundStyle(statusAccentColor(status))
      .padding(.horizontal, 10)
      .padding(.vertical, 6)
      .background(statusAccentColor(status).opacity(0.16), in: Capsule())
  }
}

private struct LeadMetaChip: View {
  let text: String
  let systemImage: String

  var body: some View {
    Label(text, systemImage: systemImage)
      .font(.system(size: 12, weight: .semibold, design: .rounded))
      .foregroundStyle(.white.opacity(0.86))
      .padding(.horizontal, 10)
      .padding(.vertical, 7)
      .background(Color.white.opacity(0.08), in: Capsule())
  }
}

private struct ContactActionButton: View {
  let title: String
  let systemImage: String
  var fullWidth = false

  var body: some View {
    HStack(spacing: 8) {
      Image(systemName: systemImage)
      Text(title)
        .lineLimit(1)
    }
    .font(.system(size: 14, weight: .bold, design: .rounded))
    .foregroundStyle(.black)
    .frame(maxWidth: fullWidth ? .infinity : nil)
    .padding(.horizontal, 14)
    .padding(.vertical, 14)
    .background(Color.white, in: RoundedRectangle(cornerRadius: 16, style: .continuous))
  }
}

private struct TrackingRow: View {
  let label: String
  let value: String

  var body: some View {
    VStack(alignment: .leading, spacing: 4) {
      Text(label.uppercased())
        .font(.system(size: 11, weight: .bold, design: .rounded))
        .foregroundStyle(.white.opacity(0.54))

      Text(value)
        .font(.system(size: 14, weight: .medium, design: .rounded))
        .foregroundStyle(.white)
        .textSelection(.enabled)
    }
  }
}

private struct ActivityRow: View {
  let activity: CRMLeadActivity

  var body: some View {
    VStack(alignment: .leading, spacing: 6) {
      HStack {
        Text(activity.title)
          .font(.system(size: 14, weight: .bold, design: .rounded))
          .foregroundStyle(.white)

        Spacer()

        Text(formatDateTime(activity.createdAt))
          .font(.system(size: 12, weight: .medium, design: .rounded))
          .foregroundStyle(.white.opacity(0.56))
      }

      if !activity.body.isEmpty {
        Text(activity.body)
          .font(.system(size: 13, weight: .medium, design: .rounded))
          .foregroundStyle(.white.opacity(0.76))
          .fixedSize(horizontal: false, vertical: true)
      }
    }
    .padding(16)
    .background(Color.white.opacity(0.08), in: RoundedRectangle(cornerRadius: 18, style: .continuous))
  }
}

private struct CRMLoadingCard: View {
  let title: String
  let message: String

  var body: some View {
    VStack(alignment: .leading, spacing: 12) {
      Text(title)
        .font(.system(size: 18, weight: .bold, design: .rounded))
        .foregroundStyle(.white)

      Text(message)
        .font(.system(size: 14, weight: .medium, design: .rounded))
        .foregroundStyle(.white.opacity(0.72))
        .fixedSize(horizontal: false, vertical: true)
    }
    .padding(20)
    .background(crmGlassCard)
  }
}

private enum BannerTone {
  case warning
  case error
  case success
}

private func infoBanner(title: String, body: String, tone: BannerTone) -> some View {
  VStack(alignment: .leading, spacing: 8) {
    Text(title)
      .font(.system(size: 13, weight: .bold, design: .rounded))
      .foregroundStyle(bannerAccentColor(tone))

    Text(body)
      .font(.system(size: 13, weight: .medium, design: .rounded))
      .foregroundStyle(.white.opacity(0.86))
      .fixedSize(horizontal: false, vertical: true)
  }
  .padding(14)
  .background(bannerAccentColor(tone).opacity(0.14), in: RoundedRectangle(cornerRadius: 16, style: .continuous))
}

private var crmBackground: some View {
  LinearGradient(
    colors: [
      Color(red: 0.06, green: 0.07, blue: 0.09),
      Color(red: 0.1, green: 0.11, blue: 0.14),
      Color(red: 0.27, green: 0.1, blue: 0.1),
    ],
    startPoint: .topLeading,
    endPoint: .bottomTrailing
  )
  .overlay(
    Circle()
      .fill(Color.white.opacity(0.08))
      .frame(width: 320, height: 320)
      .blur(radius: 90)
      .offset(x: 160, y: -160)
  )
  .ignoresSafeArea()
}

private var crmGlassCard: some ShapeStyle {
  AnyShapeStyle(
    LinearGradient(
      colors: [
        Color.white.opacity(0.11),
        Color.white.opacity(0.06),
      ],
      startPoint: .topLeading,
      endPoint: .bottomTrailing
    )
  )
}

private func statusAccentColor(_ status: String) -> Color {
  switch status {
  case "new":
    return Color(red: 0.64, green: 0.86, blue: 1.0)
  case "contacted":
    return Color(red: 1.0, green: 0.83, blue: 0.45)
  case "quoted":
    return Color(red: 1.0, green: 0.62, blue: 0.28)
  case "interview_requested":
    return Color(red: 1.0, green: 0.83, blue: 0.45)
  case "interview_scheduled":
    return Color(red: 0.51, green: 0.89, blue: 0.72)
  case "booked":
    return Color(red: 0.51, green: 0.89, blue: 0.72)
  case "won":
    return Color(red: 0.56, green: 0.94, blue: 0.54)
  case "lost":
    return Color(red: 1.0, green: 0.55, blue: 0.55)
  case "archived":
    return Color(red: 0.76, green: 0.8, blue: 0.86)
  default:
    return .white
  }
}

private func bannerAccentColor(_ tone: BannerTone) -> Color {
  switch tone {
  case .warning:
    return Color(red: 1.0, green: 0.78, blue: 0.36)
  case .error:
    return Color(red: 1.0, green: 0.52, blue: 0.52)
  case .success:
    return Color(red: 0.59, green: 0.93, blue: 0.68)
  }
}

private func applicantSourceLabel(_ applicant: CRMApplicant) -> String {
  let sourceLabel = applicant.sourceLabel.trimmingCharacters(in: .whitespacesAndNewlines)
  if !sourceLabel.isEmpty {
    return sourceLabel
  }

  switch applicant.sourceType {
  case "assistant_chat_job":
    return "Website hiring assistant"
  case "assistant_whatsapp_job":
    return "WhatsApp hiring assistant"
  case "whatsapp_job":
    return "WhatsApp hiring"
  default:
    if !applicant.tracking.sourceSummary.isEmpty {
      return applicant.tracking.sourceSummary
    }

    return applicant.sourceType.replacingOccurrences(of: "_", with: " ").capitalized
  }
}

private func applicantAnswerLabel(_ value: String) -> String {
  switch value.trimmingCharacters(in: .whitespacesAndNewlines).lowercased() {
  case "yes":
    return "Yes"
  case "no":
    return "No"
  default:
    return value.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty ? "Pending" : value
  }
}

private func leadSourceLabel(_ lead: CRMLead) -> String {
  if lead.tracking.isPaidTraffic {
    if !lead.tracking.campaignSummary.isEmpty {
      return "Google Ads · \(lead.tracking.campaignSummary)"
    }
    return "Google Ads"
  }

  switch lead.sourceType {
  case "assistant_chat":
    return "Assistant chat"
  case "assistant_booking":
    return "Assistant callback"
  case "website_form":
    return "Website form"
  case "field_prospector":
    return "Field prospector"
  case "lead_distribution_prospector":
    return "Prospector intake"
  default:
    if !lead.tracking.sourceSummary.isEmpty {
      return lead.tracking.sourceSummary
    }
    return lead.sourceType.replacingOccurrences(of: "_", with: " ").capitalized
  }
}

private func callbackSummaryText(for lead: CRMLead) -> String? {
  if !lead.nextActionAt.isEmpty {
    return "Follow-up \(formatDateTime(lead.nextActionAt))"
  }

  let bestDay = formatCalendarDay(lead.bestContactDay)
  let pieces = [bestDay, lead.bestContactTime]
    .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
    .filter { !$0.isEmpty }

  guard !pieces.isEmpty else {
    return nil
  }

  return pieces.joined(separator: " · ")
}

private func normalizedDateOnly(_ value: String) -> String {
  guard let date = AppDateFormatting.apiDateOnly.date(from: value) ??
    AppDateFormatting.apiDateTime.date(from: value) ??
    ISO8601DateFormatter().date(from: value) else {
    return value.trimmingCharacters(in: .whitespacesAndNewlines)
  }

  return AppDateFormatting.apiDateOnly.string(from: date)
}

private func buildFieldIntakeRows(for lead: CRMLead) -> [(label: String, value: String)] {
  let rows: [(String, String)] = [
    ("Street address", lead.addressLine),
    ("City / ZIP", [lead.city, lead.zipCode]
      .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
      .filter { !$0.isEmpty }
      .joined(separator: " · ")),
    ("Property type", lead.propertyType),
    ("Project size", lead.projectSize),
    ("Timeline", lead.timeline),
    ("Decision maker", lead.ownershipStatus),
    ("Budget", lead.budgetRange),
    ("Urgency", lead.urgency.capitalized),
    ("Best contact window", lead.bestContactWindow),
    ("Language", lead.preferredLanguage),
    ("Lead tier", lead.qualificationTier),
    ("Qualification note", lead.qualificationNotes),
    ("Prospector", lead.sourceProspectorName),
    ("Prospector email", lead.sourceProspectorEmail),
  ]

  return rows.filter { !$0.1.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty }
}

private func buildApplicantProfileRows(for applicant: CRMApplicant) -> [(label: String, value: String)] {
  let rows: [(String, String)] = [
    ("Position", applicant.roleLabel),
    ("Languages", applicant.languages),
    ("Years experience", applicant.yearsExperience),
    ("Own tools", applicantAnswerLabel(applicant.hasTools)),
    ("Transportation", applicantAnswerLabel(applicant.hasTransportation)),
    ("Field ready", applicantAnswerLabel(applicant.fieldReady)),
    ("Location", applicant.location),
    ("Next action", applicant.nextAction),
    ("Interview requested", formatDateTime(applicant.interviewRequestedAt)),
    ("Interview time", formatDateTime(applicant.nextActionAt)),
  ]

  return rows.filter { !$0.1.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty }
}

private func labelForStatus(_ value: String, options: [CRMStatusOption]) -> String {
  options.first(where: { $0.value == value })?.label ?? value.capitalized
}

private func formatDateTime(_ value: String) -> String {
  guard let date = AppDateFormatting.apiDateTime.date(from: value) ?? ISO8601DateFormatter().date(from: value) else {
    return value
  }

  let formatter = DateFormatter()
  formatter.timeZone = AppConfig.chicagoTimeZone
  formatter.dateStyle = .medium
  formatter.timeStyle = .short
  return formatter.string(from: date)
}

private func formatDateOnly(_ value: String) -> String {
  guard let date = AppDateFormatting.apiDateOnly.date(from: value) ??
    AppDateFormatting.apiDateTime.date(from: value) ??
    ISO8601DateFormatter().date(from: value) else {
    return value
  }

  let formatter = DateFormatter()
  formatter.timeZone = AppConfig.chicagoTimeZone
  formatter.dateStyle = .medium
  formatter.timeStyle = .none
  return formatter.string(from: date)
}

private func formatCalendarDay(_ value: String) -> String {
  guard value.count == 10 else {
    return value
  }

  let formatter = DateFormatter()
  formatter.dateFormat = "yyyy-MM-dd"
  formatter.timeZone = AppConfig.chicagoTimeZone

  guard let date = formatter.date(from: value) else {
    return value
  }

  let output = DateFormatter()
  output.timeZone = AppConfig.chicagoTimeZone
  output.dateStyle = .medium
  output.timeStyle = .none
  return output.string(from: date)
}

private extension View {
  func crmFieldStyle() -> some View {
    self
      .padding(.horizontal, 14)
      .padding(.vertical, 14)
      .background(Color.white.opacity(0.08), in: RoundedRectangle(cornerRadius: 16, style: .continuous))
      .foregroundStyle(.white)
      .font(.system(size: 15, weight: .medium, design: .rounded))
  }
}
