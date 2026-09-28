package Client;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.event.ItemEvent;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.SwingUtilities;
import javax.swing.table.DefaultTableModel;

import Common.Constants;
import Common.Job;
import Common.JobResult;
import Common.JobType;

/*
    Client GUI that displays current operations. A pop-up window appears with 
    operations that can be done within it.

*/
public class ClientGUI extends JFrame implements ClientEventListener{
    private final ClientGUIService clientService = new ClientGUIService(this);

    private JTextField hostField;
    private JTextField portField;
    private JTextField numbersField;
    private JTextField rangeStartField;
    private JTextField rangeEndField;

    private JButton loadCsvButton;
    private JButton submitButton;
    private JButton connectButton;

    private JLabel connectionStatusLabel;
    private JLabel jobsSubmittedLabel;

    private JComboBox<JobType> jobTypeCombo;

    private DefaultTableModel resultsModel;
    private JTable resultsTable;

    private int jobsSubmitted = 0;

    public ClientGUI() {
        super("Distributed Job Client");

        buildUI();
        updateJobTypeFieldState();

        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setSize(780, 560);
        setLocationRelativeTo(null);
    }

    private void buildUI() {
        setLayout(new BorderLayout(8, 8));
        add(constructConnectionPanel(), BorderLayout.NORTH);
        add(constructJobPanel(), BorderLayout.WEST);
        add(constructResultsPanel(), BorderLayout.CENTER);
    }

    private JPanel constructConnectionPanel() {
        JPanel panel = new JPanel(new FlowLayout(FlowLayout.LEFT));
        panel.setBorder(BorderFactory.createTitledBorder("Coordinator Connection"));

        hostField = new JTextField("localhost", 12);
        portField = new JTextField(String.valueOf(Constants.RMI_REGISTRY_PORT), 5);
        connectButton = new JButton("Connect");
        connectionStatusLabel = new JLabel("Not connected");
        connectionStatusLabel.setForeground(Color.RED);

        connectButton.addActionListener(e -> onClickedConnect());

        panel.add(new JLabel("Host:"));
        panel.add(hostField);
        panel.add(new JLabel("Port:"));
        panel.add(portField);
        panel.add(connectButton);
        panel.add(connectionStatusLabel);
        return panel;
    }

    private JPanel constructJobPanel() {
        JPanel panel = new JPanel();
        panel.setLayout(new BoxLayout(panel, BoxLayout.Y_AXIS));
        panel.setBorder(BorderFactory.createTitledBorder("Submit Job"));
        panel.setPreferredSize(new Dimension(300, 0));

        jobTypeCombo = new JComboBox<>(JobType.values());
        jobTypeCombo.addItemListener(e -> {
            if (e.getStateChange() == ItemEvent.SELECTED) {
                updateJobTypeFieldState();
            }
        });

        numbersField = new JTextField();
        loadCsvButton = new JButton("Load CSV...");
        loadCsvButton.addActionListener(e -> loadCsv());

        rangeStartField = new JTextField();
        rangeEndField = new JTextField();

        submitButton = new JButton("Submit Job");
        submitButton.addActionListener(e -> onClickedSubmit());

        jobsSubmittedLabel = new JLabel("Jobs submitted: 0");

        panel.add(labeled("Job Type:", jobTypeCombo));
        panel.add(Box.createVerticalStrut(8));
        panel.add(labeled("Numbers (comma separated):", numbersField));
        panel.add(loadCsvButton);
        panel.add(Box.createVerticalStrut(8));
        panel.add(labeled("Range Start:", rangeStartField));
        panel.add(labeled("Range End:", rangeEndField));
        panel.add(Box.createVerticalStrut(12));
        panel.add(submitButton);
        panel.add(Box.createVerticalStrut(8));
        panel.add(jobsSubmittedLabel);
        panel.add(Box.createVerticalGlue());

        return panel;
    }

    private JPanel labeled(String label, JComponent field) {
        JPanel p = new JPanel(new BorderLayout(4, 2));
        p.add(new JLabel(label), BorderLayout.NORTH);
        p.add(field, BorderLayout.CENTER);
        p.setMaximumSize(new Dimension(Integer.MAX_VALUE, 50));
        return p;
    }

    private JPanel constructResultsPanel() {
        JPanel panel = new JPanel(new BorderLayout());
        panel.setBorder(BorderFactory.createTitledBorder("Job Results"));

        resultsModel = new DefaultTableModel(
                new Object[]{"Job ID", "Type", "Numbers/Range", "Result", "Worker ID", "Status"}, 0) {
            @Override
            public boolean isCellEditable(int row, int column) {
                return false;
            }
        };
        resultsTable = new JTable(resultsModel);
        panel.add(new JScrollPane(resultsTable), BorderLayout.CENTER);
        return panel;
    }

    private void updateJobTypeFieldState() {
        JobType type = (JobType) jobTypeCombo.getSelectedItem();
        boolean needsNumbers = (type == JobType.MAX);
        boolean needsRange = (type == JobType.PRIMESUM || type == JobType.PRIMECOUNT);

        numbersField.setEnabled(needsNumbers);
        loadCsvButton.setEnabled(needsNumbers);
        rangeStartField.setEnabled(needsRange);
        rangeEndField.setEnabled(needsRange);
    }

    private void onClickedConnect() {
        String host = hostField.getText().trim();
        int port = 0;
        try {
            port = Integer.parseInt(portField.getText().trim());
        } catch (NumberFormatException e) {
            showErrorMsg("Port must be a number");
        }
        clientService.connect(host, port);
    }

    private void loadCsv() {
        JFileChooser chooser = new JFileChooser();
        chooser.setDialogTitle("Load numbers from CSV");
        int choice = chooser.showOpenDialog(this);
        if (choice != JFileChooser.APPROVE_OPTION) {
            return;
        }

        File file = chooser.getSelectedFile();
        List<Integer> numbers = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                for (String token : line.split(",")) {
                    token = token.trim();
                    if (token.isEmpty()) continue;
                    try {
                        numbers.add(Integer.valueOf(token));
                    } catch (NumberFormatException ignored) {
                    }
                }
            }
        } catch (IOException ex) {
            showErrorMsg("Failed to read CSV: " + ex.getMessage());
            return;
        }

        if (numbers.isEmpty()) {
            showErrorMsg("No numeric values found in that file.");
            return;
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < numbers.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(numbers.get(i));
        }
        numbersField.setText(sb.toString());
    }

    private void onClickedSubmit() {
        if(!clientService.isConnected()) {
            showErrorMsg("Connect to the Coordinator first");
        }

        JobType type = (JobType) jobTypeCombo.getSelectedItem();
        List<Integer> numbers = new ArrayList<>();
        int rangeStart = 0;
        int rangeEnd = 0;
        String summary;

        try {
            if (type == JobType.MAX) {
                for (String token : numbersField.getText().split(",")) {
                    token = token.trim();
                    if (!token.isEmpty()) {
                        numbers.add(Integer.valueOf(token));
                    }
                }
                if (numbers.isEmpty()) {
                    showErrorMsg("Enter at least one number.");
                    return;
                }
                summary = numbers.toString();
            } else {
                rangeStart = Integer.parseInt(rangeStartField.getText().trim());
                rangeEnd = Integer.parseInt(rangeEndField.getText().trim());
                if (rangeStart > rangeEnd) {
                    showErrorMsg("Range start must be <= range end.");
                    return;
                }
                summary = rangeStart + " - " + rangeEnd;
            }
        } catch (NumberFormatException ex) {
            showErrorMsg("Please enter valid whole numbers.");
            return;
        }

        String jobID = UUID.randomUUID().toString();
        Job job = new Job(jobID, type, numbers, rangeStart, rangeEnd);

        resultsModel.addRow(new Object[]{jobID, type, summary, "-", "-", "...Finding Coordinator..."});
        jobsSubmitted++;
        jobsSubmittedLabel.setText("Jobs submitted: " + jobsSubmitted);

        numbersField.setText("");
        clientService.submitJob(job);
    }

    private void updateRowStatus(String jobID, String status) {
        for (int row = 0; row < resultsModel.getRowCount(); row++) {
            if (resultsModel.getValueAt(row, 0).equals(jobID)) {
                resultsModel.setValueAt(status, row, 5);
                break;
            }
        }
    }

    private void showErrorMsg(String message) {
        JOptionPane.showMessageDialog(this, message, "Error", JOptionPane.ERROR_MESSAGE);
    }

    @Override 
    public void onConnected(String host, int port) {
        SwingUtilities.invokeLater(() -> {
            connectionStatusLabel.setText("Connected to " + host + ":" + port);
            connectionStatusLabel.setForeground(Color.GREEN);
        });
    }

    @Override 
    public void onConnectionFailed(String msg) {
        SwingUtilities.invokeLater(() -> showErrorMsg("Could not connect " + msg));
    }

    @Override
    public void onJobStatusChange(String jobID, String status) {
        SwingUtilities.invokeLater(() -> updateRowStatus(jobID, status));

    }

    @Override 
    public void onJobResult(JobResult result) {
        SwingUtilities.invokeLater(() -> {
            Object displayResult = result.isFailed() ? "Failed" + result.getErrorMessage() : result.getResult();
            Object displayWorkerID = result.isFailed() ? "-" : result.getWorkerID();
            String status = result.isFailed() ? "Failed" : "Completed";

            for (int row = 0; row < resultsModel.getRowCount(); row++) {
                if(resultsModel.getValueAt(row, 0).equals(result.getJobID())) {
                    resultsModel.setValueAt(displayResult, row, 3);
                    resultsModel.setValueAt(displayWorkerID, row, 4);
                    resultsModel.setValueAt(status, row, 5);
                    return;
                }
            }            
            resultsModel.addRow(new Object[]{result.getJobID(), "-", "-", displayResult,
                displayWorkerID, status});
        });
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
            ClientGUI client = new ClientGUI();
            client.setVisible(true);
        });
    }
}