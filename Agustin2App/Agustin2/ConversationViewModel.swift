import Foundation

@MainActor
final class CRMStore: ObservableObject {
  @Published var isBootstrapping = false
  @Published var isAuthenticated = false
  @Published var isLoggingIn = false
  @Published var isRefreshingDashboard = false
  @Published var isRefreshingAgenda = false
  @Published var isRefreshingApplicants = false
  @Published var isLoadingLead = false
  @Published var isLoadingApplicant = false
  @Published var isSavingLead = false
  @Published var isSavingApplicant = false

  @Published var loginEmail = ""
  @Published var loginPassword = ""
  @Published var sessionMessage = ""
  @Published var dashboardMessage = ""
  @Published var agendaMessage = ""
  @Published var applicantsMessage = ""
  @Published var detailMessage = ""
  @Published var applicantDetailMessage = ""
  @Published var saveFeedback = ""
  @Published var applicantSaveFeedback = ""

  @Published var statusFilter = ""
  @Published var searchText = ""
  @Published var paidOnly = false
  @Published var applicantStatusFilter = ""
  @Published var applicantSearchText = ""
  @Published var applicantRoleFilter = ""
  @Published var freshLeadIDs: Set<String> = []
  @Published var inboxAlertMessage = ""
  @Published var inboxAlertToken = 0

  @Published var editableStatus = "new"
  @Published var editableFullName = ""
  @Published var editablePhoneDisplay = ""
  @Published var editableEmail = ""
  @Published var editableProjectType = ""
  @Published var editableLocation = ""
  @Published var editableDetails = ""
  @Published var editableBestContactDay = ""
  @Published var editableBestContactTime = ""
  @Published var workDateEnabled = false
  @Published var editableClientDocumentWorkDate = Calendar.current.date(byAdding: .day, value: 1, to: .now) ?? .now
  @Published var editableNextAction = ""
  @Published var followUpDateEnabled = false
  @Published var editableNextActionAt = Calendar.current.date(byAdding: .hour, value: 2, to: .now) ?? .now
  @Published var editablePrivateNotes = ""
  @Published var editableNote = ""

  @Published var editableApplicantStatus = "new"
  @Published var editableApplicantFullName = ""
  @Published var editableApplicantPhoneDisplay = ""
  @Published var editableApplicantEmail = ""
  @Published var editableApplicantPositionApplied = ""
  @Published var editableApplicantLanguages = ""
  @Published var editableApplicantYearsExperience = ""
  @Published var editableApplicantExperienceSummary = ""
  @Published var editableApplicantHasTools = ""
  @Published var editableApplicantHasTransportation = ""
  @Published var editableApplicantFieldReady = ""
  @Published var editableApplicantLocation = ""
  @Published var editableApplicantBestInterviewDay = ""
  @Published var editableApplicantBestInterviewTime = ""
  @Published var editableApplicantNextAction = ""
  @Published var applicantFollowUpDateEnabled = false
  @Published var editableApplicantNextActionAt = Calendar.current.date(byAdding: .day, value: 1, to: .now) ?? .now
  @Published var editableApplicantPrivateNotes = ""
  @Published var editableApplicantNote = ""

  @Published var me: CRMMeResponse?
  @Published var dashboard: CRMDashboardResponse?
  @Published var agendaLeads: [CRMLead] = []
  @Published var applicants: [CRMApplicant] = []
  @Published var leadDetail: CRMLeadDetailResponse?
  @Published var applicantDetail: CRMApplicantDetailResponse?

  private var didBootstrap = false
  private var hasDashboardBaseline = false
  private var knownLeadIDs: Set<String> = []
  private var dashboardDetectionKey = ""
  private var alertClearTask: Task<Void, Never>?
  private let service: CRMService

  init(service: CRMService = CRMService()) {
    self.service = service
  }

  var displayName: String {
    let candidate = me?.profile.displayName.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
    if !candidate.isEmpty {
      return candidate
    }

    return me?.email.isEmpty == false ? me?.email ?? "CRM Admin" : "CRM Admin"
  }

  var themeLabel: String {
    me?.profile.themeLabel ?? ""
  }

  var resourceSections: [CRMResourceSection] {
    me?.resourceSections ?? []
  }

  var isConfigured: Bool {
    me?.configured ?? true
  }

  var statusOptions: [CRMStatusOption] {
    let options = dashboard?.statusOptions ?? []

    if options.isEmpty {
      return [
        CRMStatusOption(value: "new", label: "New"),
        CRMStatusOption(value: "contacted", label: "Contacted"),
        CRMStatusOption(value: "quoted", label: "Quoted"),
        CRMStatusOption(value: "booked", label: "Booked"),
        CRMStatusOption(value: "won", label: "Won"),
        CRMStatusOption(value: "lost", label: "Lost"),
        CRMStatusOption(value: "archived", label: "Archived"),
      ]
    }

    return options
  }

  var applicantStatusOptions: [CRMStatusOption] {
    [
      CRMStatusOption(value: "", label: "All statuses"),
      CRMStatusOption(value: "new", label: "New candidate"),
      CRMStatusOption(value: "interview_requested", label: "Interview requested"),
      CRMStatusOption(value: "interview_scheduled", label: "Interview scheduled"),
      CRMStatusOption(value: "archived", label: "Archived"),
    ]
  }

  var applicantRoleOptions: [String] {
    let roles = applicants
      .map { $0.roleLabel.trimmingCharacters(in: .whitespacesAndNewlines) }
      .filter { !$0.isEmpty && $0 != "Role pending" }

    return Array(Set(roles)).sorted { lhs, rhs in
      lhs.localizedCaseInsensitiveCompare(rhs) == .orderedAscending
    }
  }

  var visibleLeads: [CRMLead] {
    let leads = dashboard?.leads ?? []

    guard paidOnly else {
      return leads
    }

    return leads.filter { $0.tracking.isPaidTraffic }
  }

  var paidLeadCount: Int {
    (dashboard?.leads ?? []).filter { $0.tracking.isPaidTraffic }.count
  }

  var callbackLeadCount: Int {
    (dashboard?.leads ?? []).filter { !$0.nextActionAt.isEmpty }.count
  }

  var visibleApplicants: [CRMApplicant] {
    applicants.filter { applicant in
      let statusNeedle = applicantStatusFilter.trimmingCharacters(in: .whitespacesAndNewlines)
      if !statusNeedle.isEmpty && applicant.status != statusNeedle {
        return false
      }

      let roleNeedle = applicantRoleFilter.trimmingCharacters(in: .whitespacesAndNewlines)
      if !roleNeedle.isEmpty && applicant.roleLabel != roleNeedle {
        return false
      }

      let searchNeedle = applicantSearchText.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
      guard !searchNeedle.isEmpty else {
        return true
      }

      let haystack = [
        applicant.displayName,
        applicant.phoneDisplay,
        applicant.email,
        applicant.roleLabel,
        applicant.languages,
        applicant.yearsExperience,
        applicant.experienceSummary,
        applicant.location,
        applicant.detailsSummary,
      ]
        .joined(separator: " ")
        .lowercased()

      return haystack.contains(searchNeedle)
    }
  }

  var applicantNewCount: Int {
    applicants.filter { $0.status == "new" }.count
  }

  var applicantInterviewCount: Int {
    applicants.filter { ["interview_requested", "interview_scheduled"].contains($0.status) }.count
  }

  var applicantWithPhoneCount: Int {
    applicants.filter { !$0.phoneDigits.isEmpty }.count
  }

  var upcomingAgendaLeads: [CRMLead] {
    agendaLeads
      .filter { lead in
        lead.hasScheduledWorkDate &&
          !isAgendaLeadActive(lead) &&
          !["lost", "archived"].contains(lead.status)
      }
      .sorted(by: compareAgendaSchedule)
  }

  var agendaNeedsDateLeads: [CRMLead] {
    agendaLeads
      .filter { $0.status == "booked" && !$0.hasScheduledWorkDate }
      .sorted(by: compareAgendaFreshness)
  }

  var activeProjectLeads: [CRMLead] {
    agendaLeads
      .filter(isAgendaLeadActive)
      .sorted(by: compareAgendaSchedule)
  }

  var currentLead: CRMLead? {
    leadDetail?.lead
  }

  var currentLeadAssets: [CRMLeadAsset] {
    leadDetail?.assets ?? []
  }

  var currentLeadActivity: [CRMLeadActivity] {
    leadDetail?.activity ?? []
  }

  var currentApplicant: CRMApplicant? {
    applicantDetail?.applicant
  }

  var currentApplicantActivity: [CRMLeadActivity] {
    applicantDetail?.activity ?? []
  }

  func bootstrapIfNeeded() async {
    guard !didBootstrap else {
      return
    }

    didBootstrap = true
    await refreshSession()
  }

  func refreshSession() async {
    isBootstrapping = true
    sessionMessage = ""

    defer {
      isBootstrapping = false
    }

    do {
      let me = try await service.me()
      self.me = me
      if !me.allowedEmail.isEmpty {
        loginEmail = me.allowedEmail
      }
      isAuthenticated = me.authenticated

      if me.authenticated {
        await refreshWorkspace()
      }
    } catch {
      sessionMessage = describe(error, fallback: "I could not verify the CRM session.")
      isAuthenticated = false
    }
  }

  func login() async {
    guard !loginEmail.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
      sessionMessage = "Enter the CRM email first."
      return
    }

    guard !loginPassword.isEmpty else {
      sessionMessage = "Enter the CRM password."
      return
    }

    isLoggingIn = true
    sessionMessage = ""

    defer {
      isLoggingIn = false
    }

    do {
      _ = try await service.login(
        email: loginEmail.trimmingCharacters(in: .whitespacesAndNewlines),
        password: loginPassword
      )
      loginPassword = ""
      let me = try await service.me()
      self.me = me
      isAuthenticated = me.authenticated
      await refreshWorkspace()
    } catch {
      sessionMessage = describe(error, fallback: "I could not sign into the CRM.")
      isAuthenticated = false
    }
  }

  func logout() async {
    do {
      try await service.logout()
    } catch {
      sessionMessage = describe(error, fallback: "I could not close the session cleanly.")
    }

    dashboard = nil
    agendaLeads = []
    applicants = []
    leadDetail = nil
    applicantDetail = nil
    saveFeedback = ""
    applicantSaveFeedback = ""
    detailMessage = ""
    applicantDetailMessage = ""
    dashboardMessage = ""
    agendaMessage = ""
    applicantsMessage = ""
    loginPassword = ""
    freshLeadIDs = []
    inboxAlertMessage = ""
    inboxAlertToken = 0
    knownLeadIDs = []
    hasDashboardBaseline = false
    dashboardDetectionKey = ""
    alertClearTask?.cancel()
    alertClearTask = nil
    isAuthenticated = false
  }

  func loadDashboard() async {
    guard isAuthenticated else {
      return
    }

    guard !isRefreshingDashboard else {
      return
    }

    isRefreshingDashboard = true
    dashboardMessage = ""

    defer {
      isRefreshingDashboard = false
    }

    do {
      let queryKey = dashboardQueryKey(
        status: statusFilter,
        search: searchText.trimmingCharacters(in: .whitespacesAndNewlines)
      )
      let dashboard = try await service.dashboard(
        status: statusFilter,
        search: searchText.trimmingCharacters(in: .whitespacesAndNewlines)
      )
      detectIncomingLeads(in: dashboard.leads, queryKey: queryKey)
      self.dashboard = dashboard
      statusFilter = dashboard.filters.status

      if let currentLeadId = leadDetail?.lead.id,
         !dashboard.leads.contains(where: { $0.id == currentLeadId }) {
        leadDetail = nil
      }
    } catch {
      handleProtectedError(error, fallback: "I could not load the lead inbox.")
      dashboardMessage = describe(error, fallback: "I could not load the lead inbox.")
    }
  }

  func loadAgenda() async {
    guard isAuthenticated else {
      return
    }

    guard !isRefreshingAgenda else {
      return
    }

    isRefreshingAgenda = true
    agendaMessage = ""

    defer {
      isRefreshingAgenda = false
    }

    do {
      let snapshot = try await service.dashboard()
      agendaLeads = snapshot.leads
    } catch {
      handleProtectedError(error, fallback: "I could not load the work agenda.")
      agendaMessage = describe(error, fallback: "I could not load the work agenda.")
    }
  }

  func loadApplicants() async {
    guard isAuthenticated else {
      return
    }

    guard !isRefreshingApplicants else {
      return
    }

    isRefreshingApplicants = true
    applicantsMessage = ""

    defer {
      isRefreshingApplicants = false
    }

    do {
      let response = try await service.applicants()
      applicants = response.applicants

      if let currentApplicantId = applicantDetail?.applicant.id,
         !response.applicants.contains(where: { $0.id == currentApplicantId }) {
        applicantDetail = nil
      }
    } catch {
      handleProtectedError(error, fallback: "I could not load applicants.")
      applicantsMessage = describe(error, fallback: "I could not load applicants.")
    }
  }

  func refreshWorkspace() async {
    await withTaskGroup(of: Void.self) { group in
      group.addTask {
        await self.loadDashboard()
      }
      group.addTask {
        await self.loadAgenda()
      }
      group.addTask {
        await self.loadApplicants()
      }
    }
  }

  func loadLeadDetail(id: String) async {
    guard isAuthenticated else {
      return
    }

    markLeadSeen(id: id)
    isLoadingLead = true
    detailMessage = ""

    if leadDetail?.lead.id != id {
      leadDetail = nil
    }

    defer {
      isLoadingLead = false
    }

    do {
      let detail = try await service.leadDetail(id: id)
      leadDetail = detail
      populateEditor(from: detail.lead)
    } catch {
      handleProtectedError(error, fallback: "I could not open this lead.")
      detailMessage = describe(error, fallback: "I could not open this lead.")
    }
  }

  func refreshLeadDetail() async {
    guard let leadId = leadDetail?.lead.id else {
      return
    }

    await loadLeadDetail(id: leadId)
  }

  func loadApplicantDetail(id: String) async {
    guard isAuthenticated else {
      return
    }

    isLoadingApplicant = true
    applicantDetailMessage = ""

    if applicantDetail?.applicant.id != id {
      applicantDetail = nil
      applicantSaveFeedback = ""
    }

    defer {
      isLoadingApplicant = false
    }

    do {
      let detail = try await service.applicantDetail(id: id)
      applicantDetail = detail
      populateApplicantEditor(from: detail.applicant)
    } catch {
      handleProtectedError(error, fallback: "I could not open this applicant.")
      applicantDetailMessage = describe(error, fallback: "I could not open this applicant.")
    }
  }

  func refreshApplicantDetail() async {
    guard let applicantId = applicantDetail?.applicant.id else {
      return
    }

    await loadApplicantDetail(id: applicantId)
  }

  func markLeadSeen(id: String) {
    guard !id.isEmpty else {
      return
    }

    freshLeadIDs.remove(id)
  }

  func saveLead() async {
    guard let lead = leadDetail?.lead else {
      return
    }

    isSavingLead = true
    saveFeedback = ""

    defer {
      isSavingLead = false
    }

    let payload = CRMLeadUpdatePayload(
      fullName: editableFullName.trimmingCharacters(in: .whitespacesAndNewlines),
      phoneDisplay: editablePhoneDisplay.trimmingCharacters(in: .whitespacesAndNewlines),
      email: editableEmail.trimmingCharacters(in: .whitespacesAndNewlines),
      projectType: editableProjectType.trimmingCharacters(in: .whitespacesAndNewlines),
      location: editableLocation.trimmingCharacters(in: .whitespacesAndNewlines),
      details: editableDetails.trimmingCharacters(in: .whitespacesAndNewlines),
      bestContactDay: editableBestContactDay.trimmingCharacters(in: .whitespacesAndNewlines),
      bestContactTime: editableBestContactTime.trimmingCharacters(in: .whitespacesAndNewlines),
      clientDocumentWorkDate: workDateEnabled ? AppDateFormatting.apiDateOnly.string(from: editableClientDocumentWorkDate) : "",
      status: editableStatus,
      nextAction: editableNextAction.trimmingCharacters(in: .whitespacesAndNewlines),
      nextActionAt: followUpDateEnabled ? AppDateFormatting.apiDateTime.string(from: editableNextActionAt) : "",
      privateNotes: editablePrivateNotes.trimmingCharacters(in: .whitespacesAndNewlines),
      note: editableNote.trimmingCharacters(in: .whitespacesAndNewlines)
    )

    do {
      let updated = try await service.updateLead(id: lead.id, payload: payload)
      leadDetail = updated
      populateEditor(from: updated.lead)
      saveFeedback = "Lead saved."
      await refreshWorkspace()
    } catch {
      handleProtectedError(error, fallback: "I could not save this lead.")
      saveFeedback = describe(error, fallback: "I could not save this lead.")
    }
  }

  func saveApplicant() async {
    guard let applicant = applicantDetail?.applicant else {
      return
    }

    isSavingApplicant = true
    applicantSaveFeedback = ""

    defer {
      isSavingApplicant = false
    }

    let payload = CRMApplicantUpdatePayload(
      fullName: editableApplicantFullName.trimmingCharacters(in: .whitespacesAndNewlines),
      phoneDisplay: editableApplicantPhoneDisplay.trimmingCharacters(in: .whitespacesAndNewlines),
      email: editableApplicantEmail.trimmingCharacters(in: .whitespacesAndNewlines),
      positionApplied: editableApplicantPositionApplied.trimmingCharacters(in: .whitespacesAndNewlines),
      languages: editableApplicantLanguages.trimmingCharacters(in: .whitespacesAndNewlines),
      yearsExperience: editableApplicantYearsExperience.trimmingCharacters(in: .whitespacesAndNewlines),
      experienceSummary: editableApplicantExperienceSummary.trimmingCharacters(in: .whitespacesAndNewlines),
      hasTools: editableApplicantHasTools.trimmingCharacters(in: .whitespacesAndNewlines),
      hasTransportation: editableApplicantHasTransportation.trimmingCharacters(in: .whitespacesAndNewlines),
      fieldReady: editableApplicantFieldReady.trimmingCharacters(in: .whitespacesAndNewlines),
      location: editableApplicantLocation.trimmingCharacters(in: .whitespacesAndNewlines),
      bestInterviewDay: editableApplicantBestInterviewDay.trimmingCharacters(in: .whitespacesAndNewlines),
      bestInterviewTime: editableApplicantBestInterviewTime.trimmingCharacters(in: .whitespacesAndNewlines),
      status: editableApplicantStatus,
      nextAction: editableApplicantNextAction.trimmingCharacters(in: .whitespacesAndNewlines),
      nextActionAt: applicantFollowUpDateEnabled ? AppDateFormatting.apiDateTime.string(from: editableApplicantNextActionAt) : "",
      privateNotes: editableApplicantPrivateNotes.trimmingCharacters(in: .whitespacesAndNewlines),
      note: editableApplicantNote.trimmingCharacters(in: .whitespacesAndNewlines)
    )

    do {
      let updated = try await service.updateApplicant(id: applicant.id, payload: payload)
      applicantDetail = updated
      populateApplicantEditor(from: updated.applicant)
      applicantSaveFeedback = "Applicant saved."
      await refreshWorkspace()
    } catch {
      handleProtectedError(error, fallback: "I could not save this applicant.")
      applicantSaveFeedback = describe(error, fallback: "I could not save this applicant.")
    }
  }

  func assetURL(for asset: CRMLeadAsset) -> URL? {
    service.assetURL(relativePath: asset.downloadUrl)
  }

  private func populateEditor(from lead: CRMLead) {
    editableStatus = lead.status
    editableFullName = lead.sanitizedFullName
    editablePhoneDisplay = lead.phoneDisplay
    editableEmail = lead.email
    editableProjectType = lead.projectType
    editableLocation = lead.location
    editableDetails = lead.details
    editableBestContactDay = lead.bestContactDay
    editableBestContactTime = lead.bestContactTime
    editableNextAction = lead.nextAction
    editablePrivateNotes = lead.privateNotes
    editableNote = ""

    if let workDate = parseDate(lead.clientDocumentWorkDate) {
      workDateEnabled = true
      editableClientDocumentWorkDate = workDate
    } else {
      workDateEnabled = false
      editableClientDocumentWorkDate = Calendar.current.date(byAdding: .day, value: 1, to: .now) ?? .now
    }

    if let nextActionAt = parseDate(lead.nextActionAt) {
      followUpDateEnabled = true
      editableNextActionAt = nextActionAt
    } else {
      followUpDateEnabled = false
      editableNextActionAt = Calendar.current.date(byAdding: .hour, value: 2, to: .now) ?? .now
    }
  }

  private func populateApplicantEditor(from applicant: CRMApplicant) {
    editableApplicantStatus = applicant.status
    editableApplicantFullName = applicant.sanitizedFullName
    editableApplicantPhoneDisplay = applicant.phoneDisplay
    editableApplicantEmail = applicant.email
    editableApplicantPositionApplied = applicant.positionApplied
    editableApplicantLanguages = applicant.languages
    editableApplicantYearsExperience = applicant.yearsExperience
    editableApplicantExperienceSummary = applicant.experienceSummary
    editableApplicantHasTools = applicant.hasTools
    editableApplicantHasTransportation = applicant.hasTransportation
    editableApplicantFieldReady = applicant.fieldReady
    editableApplicantLocation = applicant.location
    editableApplicantBestInterviewDay = applicant.bestInterviewDay
    editableApplicantBestInterviewTime = applicant.bestInterviewTime
    editableApplicantNextAction = applicant.nextAction
    editableApplicantPrivateNotes = applicant.manualPrivateNotes
    editableApplicantNote = ""

    if let nextActionAt = parseDate(applicant.nextActionAt) {
      applicantFollowUpDateEnabled = true
      editableApplicantNextActionAt = nextActionAt
    } else {
      applicantFollowUpDateEnabled = false
      editableApplicantNextActionAt = Calendar.current.date(byAdding: .day, value: 1, to: .now) ?? .now
    }
  }

  private func handleProtectedError(_ error: Error, fallback: String) {
    guard case let CRMServiceError.unauthorized(message) = error else {
      return
    }

    sessionMessage = message.isEmpty ? fallback : message
    dashboard = nil
    agendaLeads = []
    applicants = []
    leadDetail = nil
    applicantDetail = nil
    loginPassword = ""
    isAuthenticated = false
    saveFeedback = ""
    applicantSaveFeedback = ""
  }

  private func describe(_ error: Error, fallback: String) -> String {
    if let localized = error as? LocalizedError, let description = localized.errorDescription, !description.isEmpty {
      return description
    }

    return fallback
  }

  private func parseDate(_ value: String) -> Date? {
    guard !value.isEmpty else {
      return nil
    }

    return AppDateFormatting.apiDateOnly.date(from: value) ??
      AppDateFormatting.apiDateTime.date(from: value) ??
      ISO8601DateFormatter().date(from: value)
  }

  private func detectIncomingLeads(in leads: [CRMLead], queryKey: String) {
    let incomingIDs = Set(leads.map(\.id))

    if dashboardDetectionKey != queryKey {
      dashboardDetectionKey = queryKey
      knownLeadIDs = incomingIDs
      hasDashboardBaseline = true
      freshLeadIDs = freshLeadIDs.intersection(incomingIDs)
      return
    }

    guard hasDashboardBaseline else {
      knownLeadIDs = incomingIDs
      hasDashboardBaseline = true
      return
    }

    let newLeadIDs = incomingIDs.subtracting(knownLeadIDs)
    knownLeadIDs = incomingIDs
    freshLeadIDs = freshLeadIDs.intersection(incomingIDs)

    guard !newLeadIDs.isEmpty else {
      return
    }

    freshLeadIDs.formUnion(newLeadIDs)

    let newLeads = leads.filter { newLeadIDs.contains($0.id) }
    if newLeads.count == 1, let lead = newLeads.first {
      let leadName = lead.displayName
      inboxAlertMessage = "\(leadName) just came in."
    } else {
      inboxAlertMessage = "\(newLeads.count) new leads just came in."
    }
    inboxAlertToken += 1

    alertClearTask?.cancel()
    alertClearTask = Task { @MainActor in
      try? await Task.sleep(for: .seconds(AppConfig.inboxLeadAlertDuration))
      guard !Task.isCancelled else {
        return
      }
      inboxAlertMessage = ""
    }
  }

  private func isAgendaLeadActive(_ lead: CRMLead) -> Bool {
    if lead.status == "won" {
      return true
    }

    guard !["lost", "archived"].contains(lead.status),
      let workDate = parseDate(lead.clientDocumentWorkDate) else {
      return false
    }

    var calendar = Calendar(identifier: .gregorian)
    calendar.timeZone = AppConfig.chicagoTimeZone
    let startOfTomorrow = calendar.date(byAdding: .day, value: 1, to: calendar.startOfDay(for: .now)) ?? .now
    return workDate < startOfTomorrow
  }

  private func compareAgendaSchedule(_ lhs: CRMLead, _ rhs: CRMLead) -> Bool {
    let leftDate = parseDate(lhs.clientDocumentWorkDate) ?? parseDate(lhs.updatedAt) ?? parseDate(lhs.createdAt) ?? .distantFuture
    let rightDate = parseDate(rhs.clientDocumentWorkDate) ?? parseDate(rhs.updatedAt) ?? parseDate(rhs.createdAt) ?? .distantFuture

    if leftDate != rightDate {
      return leftDate < rightDate
    }

    return lhs.displayName.localizedCaseInsensitiveCompare(rhs.displayName) == .orderedAscending
  }

  private func compareAgendaFreshness(_ lhs: CRMLead, _ rhs: CRMLead) -> Bool {
    let leftDate = parseDate(lhs.updatedAt) ?? parseDate(lhs.createdAt) ?? .distantPast
    let rightDate = parseDate(rhs.updatedAt) ?? parseDate(rhs.createdAt) ?? .distantPast

    if leftDate != rightDate {
      return leftDate > rightDate
    }

    return lhs.displayName.localizedCaseInsensitiveCompare(rhs.displayName) == .orderedAscending
  }

  private func dashboardQueryKey(status: String, search: String) -> String {
    "\(status.lowercased())|\(search.lowercased())"
  }
}
